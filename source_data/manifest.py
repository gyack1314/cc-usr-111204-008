"""以冻结的 S3 Inventory 样式 manifest 为唯一依据的版本视图。

清单分片先按声明的 gzip 字节长度与 SHA-256 核对，再以 csv 模块按 schema
给定的列顺序逐行流式解析。键只做一次百分号解码，字面加号不是空格；
version_id 是不解释字符串，字面 "null" 与缺字段/空值严格区分。
本模块不接触任何对象版本，只产出可回溯到清单行的身份集合。
"""
import csv
import gzip
import hashlib
import io
from dataclasses import dataclass
from urllib.parse import unquote

from .errors import ManifestError

_BOOL = {'true': True, 'false': False}


@dataclass(frozen=True)
class Identity:
    """版本身份三元组。version_id 为 None 仅表示清单缺列/空值（非法）。"""
    bucket: str
    key: str
    version_id: str

    def token(self):
        """与本地路径无关的稳定内部身份串。"""
        return f'{self.bucket}\x1f{self.key}\x1f{self.version_id}'


@dataclass(frozen=True)
class ManifestRow:
    identity: Identity
    is_latest: bool
    delete_marker: bool
    size: int | None
    shard_key: str
    line_number: int  # 分片 CSV 内从 1 开始的物理行号


@dataclass(frozen=True)
class Manifest:
    generation: str
    schema: tuple
    rows: tuple
    shard_digests: tuple

    def current_versions(self):
        """当前版视图：仅 is_latest 且非墓碑的行。"""
        return tuple(r for r in self.rows if r.is_latest and not r.delete_marker)

    def collectable_versions(self, collect_history):
        """按配置选出要 GET 的版本；墓碑永远不参与下载。"""
        rows = self.rows if collect_history else self.current_versions()
        return tuple(r for r in rows if not r.delete_marker)

    def tombstones(self):
        return tuple(r for r in self.rows if r.delete_marker)


def _require_schema(schema):
    if not isinstance(schema, list) or not schema:
        raise ManifestError('schema 必须是非空列名数组')
    seen = set()
    for name in schema:
        if not isinstance(name, str) or not name:
            raise ManifestError('schema 列名必须是非空字符串')
        if name in seen:
            raise ManifestError(f'schema 列名重复: {name}')
        seen.add(name)
    for required in ('bucket', 'key', 'version_id', 'is_latest', 'delete_marker'):
        if required not in seen:
            raise ManifestError(f'schema 缺少必需列: {required}')
    return tuple(schema)


def _verify_shard(root, shard):
    for field in ('key', 'file', 'bytes', 'sha256'):
        if field not in shard:
            raise ManifestError(f'分片声明缺少字段: {field}')
    if type(shard['bytes']) is not int or shard['bytes'] < 0:
        raise ManifestError('分片 bytes 必须是非负整数')
    if not isinstance(shard['sha256'], str) or len(shard['sha256']) != 64:
        raise ManifestError('分片 sha256 必须是 64 位十六进制字符串')
    try:
        int(shard['sha256'], 16)
    except ValueError:
        raise ManifestError('分片 sha256 不是十六进制')
    raw, digest = _read_shard_file(root, shard['file'])
    if len(raw) != shard['bytes']:
        raise ManifestError(
            f"分片 {shard['key']} 长度不符: 实际 {len(raw)} != 声明 {shard['bytes']}")
    if digest != shard['sha256'].lower():
        raise ManifestError(f"分片 {shard['key']} SHA-256 不符")
    return raw, digest


def _read_shard_file(root, name):
    if not isinstance(name, str) or not name:
        raise ManifestError('分片文件名为空')
    base = root.resolve()
    target = (base / name).resolve()
    if not target.is_relative_to(base) or target == base:
        raise ManifestError('分片文件路径越界')
    raw = target.read_bytes()
    return raw, hashlib.sha256(raw).hexdigest()


def _decode_key(encoded):
    # unquote 默认把加号保留为字面字符，仅做一次百分号解码。
    return unquote(encoded, encoding='utf-8', errors='strict')


def _parse_bool(value, field, where):
    if value not in _BOOL:
        raise ManifestError(f'{where} 的 {field} 只能是 true 或 false，得到 {value!r}')
    return _BOOL[value]


def _iter_rows(raw, schema, shard_key, require_size):
    """逐行流式解析已核对的 gzip 分片，不一次性展开全部 CSV 文本。"""
    where = f'分片 {shard_key}'
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            text_stream = io.TextIOWrapper(gz, encoding='utf-8',
                                           errors='strict', newline='')
            reader = csv.reader(text_stream)
            for line_number, values in enumerate(reader, start=1):
                yield _build_row(values, schema, shard_key, line_number,
                                 require_size)
    except (OSError, UnicodeDecodeError) as exc:
        raise ManifestError(f'{where} 无法解压为 UTF-8 CSV: {exc}')


def _build_row(values, schema, shard_key, line_number, require_size):
    where = f'分片 {shard_key} 第 {line_number} 行'
    if len(values) != len(schema):
        raise ManifestError(f'{where} 列数 {len(values)} 与 schema {len(schema)} 不符')
    record = dict(zip(schema, values))
    bucket = record['bucket']
    encoded_key = record['key']
    version_id = record['version_id']
    # 缺列（本不可能，长度已校验）与空身份都视为资料不完整；
    # 字面 "null" 是合法的 null 版本 ID，必须原样保留。
    if not bucket:
        raise ManifestError(f'{where} bucket 为空')
    if not encoded_key:
        raise ManifestError(f'{where} key 为空')
    if version_id == '':
        raise ManifestError(f'{where} version_id 缺失或为空，不能与 null 版本混同')
    try:
        key = _decode_key(encoded_key)
    except UnicodeDecodeError as exc:
        raise ManifestError(f'{where} key 百分号解码失败: {exc}')
    is_latest = _parse_bool(record['is_latest'], 'is_latest', where)
    delete_marker = _parse_bool(record['delete_marker'], 'delete_marker', where)
    size = None
    if require_size:
        size_text = record.get('size', '')
        if not size_text:
            raise ManifestError(f'{where} size 缺失')
        try:
            size = int(size_text)
        except ValueError:
            raise ManifestError(f'{where} size 不是整数: {size_text!r}')
        if size < 0:
            raise ManifestError(f'{where} size 为负')
    if delete_marker and is_latest and size not in (None, 0):
        raise ManifestError(f'{where} 墓碑行 size 必须为 0/缺省')
    return ManifestRow(
        identity=Identity(bucket, key, version_id),
        is_latest=is_latest,
        delete_marker=delete_marker,
        size=size,
        shard_key=shard_key,
        line_number=line_number,
    )


def _detect_conflicts(rows):
    by_identity = {}
    latest_by_key = {}
    for row in rows:
        ident = row.identity
        prior = by_identity.get(ident)
        if prior is not None:
            if (prior.is_latest, prior.delete_marker, prior.size) != (
                    row.is_latest, row.delete_marker, row.size):
                raise ManifestError(
                    f'重复身份 {ident.bucket}/{ident.key} 版本 {ident.version_id} '
                    f'元数据不一致: {prior.shard_key}:{prior.line_number} vs '
                    f'{row.shard_key}:{row.line_number}')
        else:
            by_identity[ident] = row
        if row.is_latest:
            bucket_key = (ident.bucket, ident.key)
            if bucket_key in latest_by_key and latest_by_key[bucket_key] != ident:
                raise ManifestError(
                    f'同 key {ident.bucket}/{ident.key} 存在多条 is_latest=true 版本')
            latest_by_key[bucket_key] = ident


def parse_manifest(value, root):
    """从已严格加载的 manifest JSON 与资料根目录构建冻结清单视图。"""
    if not isinstance(value, dict):
        raise ManifestError('manifest 顶层必须为对象')
    generation = value.get('generation')
    if not isinstance(generation, str) or not generation:
        raise ManifestError('generation 必须是非空字符串')
    schema = _require_schema(value.get('schema'))
    shards = value.get('shards')
    if not isinstance(shards, list) or not shards:
        raise ManifestError('shards 必须是非空数组')
    shard_keys = set()
    all_rows = []
    digests = []
    require_size = 'size' in schema
    for shard in shards:
        if not isinstance(shard, dict):
            raise ManifestError('分片声明必须为对象')
        if shard['key'] in shard_keys:
            raise ManifestError(f"分片 key 重复: {shard['key']}")
        shard_keys.add(shard['key'])
        raw, digest = _verify_shard(root, shard)
        digests.append({'key': shard['key'], 'sha256': digest,
                        'gzip_bytes': len(raw)})
        all_rows.extend(_iter_rows(raw, schema, shard['key'], require_size))
    _detect_conflicts(all_rows)
    return Manifest(
        generation=generation,
        schema=schema,
        rows=tuple(all_rows),
        shard_digests=tuple(digests),
    )
