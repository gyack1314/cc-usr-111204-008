"""按封存清单身份采集固定版本对象，并发布可回链的版本收据。

设计约束：
- 每次对象请求都固定 versionId；响应必须回显同一版本，拒绝忽略版本参数
  而回当前内容的源。
- 清单身份（manifest）、进行中对象（inflight）、已核验对象（verified）
  分三段状态记录；只有完整校验通过后才原子发布数据文件与收据。
- 崩溃恢复：已发布对象按收据复核后直接复用，临时文件永远不被当作成功。
- 本地路径由内部身份的哈希映射产生，源键不直接成为相对路径，同 key 不同
  版本不会碰撞。
"""
import hashlib
import json
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import checksum as checks
from .errors import ManifestError, SourceProtocolError, VerificationError
from .manifest import Identity, ManifestRow, parse_manifest

STATE_NAME = 'collect-state.json'
RECEIPT_SUFFIX = '.receipt.json'
_TMP_SUFFIX = '.part'


def _atomic_write(path: Path, payload: bytes):
    """同目录临时文件 + os.replace，发布要么完整可见要么不存在。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + '.', suffix=_TMP_SUFFIX, dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def local_paths(root: Path, ident: Identity):
    """内部身份 -> (数据相对路径, 收据相对路径)，与源键字符集完全解耦。"""
    digest = hashlib.sha256(ident.token().encode('utf-8')).hexdigest()
    rel = Path('objects') / digest[:2] / digest[2:4] / digest
    return root / rel, root / Path(str(rel) + RECEIPT_SUFFIX)


class CollectState:
    """三段状态的持久化载体：manifest / inflight / verified。"""

    def __init__(self, path: Path, generation: str):
        self.path = path
        self.generation = generation
        self.entries = {}
        # RLock 允许同一工作线程在持锁方法内再调用其他持锁方法。
        self._lock = threading.RLock()

    def _persist_locked(self):
        payload = {
            'generation': self.generation,
            'entries': self.entries,
        }
        _atomic_write(self.path, json.dumps(
            payload, ensure_ascii=False, indent=2, sort_keys=True).encode('utf-8'))

    def get_entry(self, token: str):
        with self._lock:
            entry = self.entries.get(token)
            return dict(entry) if entry is not None else None

    def declare(self, row: ManifestRow):
        with self._lock:
            token = row.identity.token()
            if token not in self.entries:
                self.entries[token] = {
                    'status': 'manifest',
                    'bucket': row.identity.bucket,
                    'key': row.identity.key,
                    'version_id': row.identity.version_id,
                    'is_latest': row.is_latest,
                    'delete_marker': row.delete_marker,
                    'size': row.size,
                    'provenance': {
                        'shard_key': row.shard_key,
                        'line_number': row.line_number,
                    },
                }
                self._persist_locked()

    def mark_inflight(self, token: str):
        with self._lock:
            entry = self.entries[token]
            if entry['status'] != 'verified':
                entry['status'] = 'inflight'
                self._persist_locked()

    def mark_verified(self, token: str, receipt_rel: str):
        with self._lock:
            entry = self.entries[token]
            entry['status'] = 'verified'
            entry['receipt'] = receipt_rel
            entry.pop('error', None)
            self._persist_locked()

    def mark_tombstone(self, token: str):
        with self._lock:
            entry = self.entries[token]
            entry['status'] = 'tombstone'
            self._persist_locked()

    def reset_to_manifest(self, token: str):
        """收据复核不过时回退到清单身份状态，等待本轮重新采集。"""
        with self._lock:
            entry = self.entries[token]
            entry['status'] = 'manifest'
            entry.pop('receipt', None)
            entry.pop('error', None)
            self._persist_locked()

    def mark_failed(self, token: str, message: str):
        with self._lock:
            entry = self.entries[token]
            if entry['status'] != 'verified':
                entry['status'] = 'failed'
                entry['error'] = message
                self._persist_locked()

    @classmethod
    def load_or_create(cls, path: Path, generation: str):
        if path.exists():
            raw = path.read_bytes()
            try:
                data = json.loads(raw.decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise VerificationError(f'状态文件损坏，无法安全恢复: {exc}')
            if data.get('generation') != generation:
                raise VerificationError(
                    f"状态文件 generation {data.get('generation')!r} 与清单 "
                    f'{generation!r} 不符，拒绝混用')
            state = cls(path, generation)
            state.entries = data.get('entries', {})
            return state
        state = cls(path, generation)
        state._persist_locked()
        return state


def _build_receipt(row, spec, response, verification, data_rel):
    return {
        'manifest_generation': None,  # 由调用方填充
        'identity': {
            'bucket': row.identity.bucket,
            'key': row.identity.key,
            'version_id': row.identity.version_id,
        },
        'manifest_row': {
            'shard_key': row.shard_key,
            'line_number': row.line_number,
            'is_latest': row.is_latest,
            'delete_marker': row.delete_marker,
            'size': row.size,
        },
        'object': {
            'bucket': response['bucket'],
            'key': response['key'],
            'version_id': response['version_id'],
            'size': response['size'],
            'etag': response.get('etag'),
            'content_sha256': verification['sha256'],
            'local_path': data_rel,
        },
        'checksum': verification,
        'declared_spec': {
            'checksum_type': spec.get('checksum_type'),
            'checksum': spec.get('checksum'),
        },
    }


class VersionedCollector:
    """编排清单解析、固定版本下载、校验链与原子发布。"""

    def __init__(self, manifest_value, data_root: Path, out_root: Path,
                 source, max_parallel=2):
        self.manifest = parse_manifest(manifest_value, data_root)
        self.data_root = data_root.resolve()
        self.out_root = out_root.resolve()
        self.source = source
        self.max_parallel = max(1, int(max_parallel))
        self.out_root.mkdir(parents=True, exist_ok=True)
        self.state = CollectState.load_or_create(
            self.out_root / STATE_NAME, self.manifest.generation)

    def _spec_for(self, ident: Identity):
        """在 manifest JSON 的 objects 声明中按完整身份查找校验规格。"""
        return self.source.spec_for(ident)

    def _fetch_verified(self, row: ManifestRow):
        ident = row.identity
        spec = self._spec_for(ident)
        if spec.get('bucket') != ident.bucket or spec.get('key') != ident.key \
                or spec.get('version_id') != ident.version_id:
            raise SourceProtocolError(f'对象规格身份与清单行不符: {ident.token()}')

        response = self.source.get_object(ident.bucket, ident.key, ident.version_id)
        self._require_version_echo(ident, response)
        content = response['body']
        if not isinstance(content, (bytes, bytearray)):
            raise SourceProtocolError('GetObject 响应体必须是字节')
        content = bytes(content)
        if row.size is not None and response['size'] != row.size:
            raise VerificationError(
                f'响应长度 {response["size"]} 与清单 size {row.size} 不符')
        if len(content) != response['size']:
            raise VerificationError('响应体实际字节数与声明 size 不符')

        pages = []
        if spec.get('checksum_type') == 'COMPOSITE':
            pages = self._fetch_all_part_pages(ident, spec)
        verification = checks.verify_object(content, spec, pages)
        return spec, response, verification, content

    def _fetch_all_part_pages(self, ident: Identity, spec):
        pages = []
        marker = None
        seen = set()
        while True:
            page = self.source.get_object_attributes(
                ident.bucket, ident.key, ident.version_id, marker)
            echo = (page.get('bucket'), page.get('key'), page.get('version_id'))
            if echo != (ident.bucket, ident.key, ident.version_id):
                raise SourceProtocolError(
                    'GetObjectAttributes 未回显请求的固定版本身份')
            token = (page.get('part_marker'), page.get('next_part_marker'),
                    len(page.get('parts', [])))
            if token in seen:
                raise SourceProtocolError('part 分页出现完全重复页，无法终止')
            seen.add(token)
            pages.append(page)
            nxt = page.get('next_part_marker')
            if nxt is None:
                break
            if not isinstance(nxt, str):
                raise SourceProtocolError('next_part_marker 必须是字符串或 null')
            marker = nxt
            if len(pages) > 10_000:
                raise SourceProtocolError('part 分页超过安全上限')
        return pages

    @staticmethod
    def _require_version_echo(ident: Identity, response):
        for field in ('bucket', 'key', 'version_id', 'size'):
            if field not in response:
                raise SourceProtocolError(f'GetObject 响应缺少字段: {field}')
        if response['bucket'] != ident.bucket or response['key'] != ident.key:
            raise SourceProtocolError('GetObject 响应身份与请求不一致')
        if response['version_id'] != ident.version_id:
            raise SourceProtocolError(
                '源忽略了 versionId 参数并返回了其他版本: '
                f'请求 {ident.version_id!r}，实际 {response["version_id"]!r}')
        if type(response['size']) is not int or response['size'] < 0:
            raise SourceProtocolError('GetObject size 必须是非负整数')

    def _verify_published(self, row: ManifestRow):
        """崩溃恢复：已发布对象按收据与磁盘字节重新复核。"""
        token = row.identity.token()
        entry = self.state.get_entry(token)
        if not entry or entry.get('status') != 'verified':
            return False
        receipt_path = self.out_root / entry['receipt']
        if not receipt_path.exists():
            self.state.reset_to_manifest(token)
            return False
        try:
            receipt = json.loads(receipt_path.read_bytes().decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise VerificationError(f'收据损坏无法复核: {token}: {exc}')
        data_path = self.out_root / receipt['object']['local_path']
        if not data_path.exists():
            self.state.reset_to_manifest(token)
            return False
        content = data_path.read_bytes()
        ident = row.identity
        if receipt['identity'] != {
                'bucket': ident.bucket, 'key': ident.key,
                'version_id': ident.version_id}:
            raise VerificationError(f'收据身份与清单不符: {token}')
        if len(content) != receipt['object']['size']:
            raise VerificationError(f'已发布对象长度与收据不符: {token}')
        digest_hex = hashlib.sha256(content).hexdigest()
        if digest_hex != receipt['object']['content_sha256']:
            raise VerificationError(f'已发布对象 SHA-256 与收据不符: {token}')
        # 收据在发布前已通过完整校验链；恢复只依据收据与本地字节复核，
        # 不重新联系源、不重新翻 part 页，避免把运行时状态混入封存判断。
        spec = self._spec_for(ident)
        declared = receipt['declared_spec']
        if declared.get('checksum_type') != spec.get('checksum_type'):
            raise VerificationError(f'收据校验类型与清单规格不符: {token}')
        return True

    def _process_one(self, row: ManifestRow):
        token = row.identity.token()
        self.state.declare(row)
        try:
            # 收据复核失败（字节被改、收据损坏等）不当作本轮采集失败，
            # 而是回退状态后重新下载，确保可自愈。
            try:
                already_published = self._verify_published(row)
            except VerificationError:
                self.state.reset_to_manifest(token)
                already_published = False
            if already_published:
                return token, 'reused'
            # 收据缺失或复核不过：状态已回退为 manifest，本轮重新采集，
            # 绝不把残留的临时/损坏文件当作成功。
            self.state.mark_inflight(token)
            spec, response, verification, content = self._fetch_verified(row)

            data_path, receipt_path = local_paths(self.out_root, row.identity)
            data_rel = data_path.relative_to(self.out_root).as_posix()
            receipt_rel = receipt_path.relative_to(self.out_root).as_posix()

            receipt = _build_receipt(row, spec, response, verification, data_rel)
            receipt['manifest_generation'] = self.manifest.generation
            # 先发布数据，再发布收据，最后翻转状态：任何时刻临时文件都不会
            # 被无收据地视为成功。
            _atomic_write(data_path, content)
            _atomic_write(receipt_path, json.dumps(
                receipt, ensure_ascii=False, indent=2, sort_keys=True).encode('utf-8'))
            self.state.mark_verified(token, receipt_rel)
            return token, 'verified'
        except (VerificationError, SourceProtocolError, ManifestError) as exc:
            self.state.mark_failed(token, str(exc))
            return token, f'failed: {exc}'
        except Exception as exc:  # 任何意外都不得丢弃状态或冒充成功
            self.state.mark_failed(token, f'{type(exc).__name__}: {exc}')
            return token, f'failed: {type(exc).__name__}: {exc}'

    def run(self, collect_history=True):
        targets = self.manifest.collectable_versions(collect_history)
        for row in self.manifest.rows:
            self.state.declare(row)
        for row in self.manifest.tombstones():
            self.state.mark_tombstone(row.identity.token())

        results = []
        failures = []
        if self.max_parallel == 1:
            for row in targets:
                token, outcome = self._process_one(row)
                results.append((token, outcome))
                if outcome.startswith('failed'):
                    failures.append((token, outcome))
        else:
            with ThreadPoolExecutor(max_workers=self.max_parallel) as pool:
                for token, outcome in pool.map(self._process_one, targets):
                    results.append((token, outcome))
                    if outcome.startswith('failed'):
                        failures.append((token, outcome))
        return {
            'generation': self.manifest.generation,
            'total_rows': len(self.manifest.rows),
            'targets': len(targets),
            'tombstones': len(self.manifest.tombstones()),
            'results': results,
            'failures': failures,
            'state_path': (self.out_root / STATE_NAME).relative_to(
                self.out_root).as_posix(),
        }
