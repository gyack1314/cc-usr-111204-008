"""对象内容校验链：FULL_OBJECT 与服务分片 COMPOSITE。

ETag 只作身份辅助记录，绝不直接当作完整内容 MD5。COMPOSITE 必须使用服务
声明的有序 part（序号、边界长度、part SHA-256），不能用本地自定的分块大小
重算替代。组合值 = SHA256(按 part number 排序后各 part 原始 32 字节摘要拼接)，
并与 "<base64>-<part 总数>" 形式的服务声明同时核对。
"""
import base64
import hashlib

from .errors import VerificationError

_FULL = 'FULL_OBJECT'
_COMPOSITE = 'COMPOSITE'
_DIGEST_LEN = 32


def _b64_digest(text, what):
    if not isinstance(text, str):
        raise VerificationError(f'{what} 必须是 base64 字符串')
    try:
        raw = base64.b64decode(text, validate=True)
    except (ValueError, TypeError):
        raise VerificationError(f'{what} 不是合法 base64')
    if len(raw) != _DIGEST_LEN:
        raise VerificationError(f'{what} 解码后不是 32 字节 SHA-256')
    return raw


def verify_full_object(content, declared_checksum, declared_sha256_hex=None):
    """完整对象：直接对全文字节算一次 SHA-256 并比对服务声明。"""
    digest = hashlib.sha256(content).digest()
    if _b64_digest(declared_checksum, 'FULL_OBJECT checksum') != digest:
        raise VerificationError('FULL_OBJECT SHA-256 与对象内容不符')
    if declared_sha256_hex is not None and digest.hex() != declared_sha256_hex.lower():
        raise VerificationError('FULL_OBJECT 十六进制 SHA-256 与服务 checksum 不一致')
    return digest.hex()


def _audit_page_chain(pages, part_count):
    """页可乱序到达：以 marker/next_part_marker 集合审计链的唯一与完整。

    规则：marker 唯一；除唯一的末页（next=null）外，每页的 next 必须指向
    一个实际取到的页；恰有一个起点 marker 不被任何 next 引用。这样乱序、
    重复页、静默截断或成环都能被发现，而不依赖页的到达顺序。
    """
    markers = []
    nexts = []
    entry_count = 0
    for page in pages:
        marker = page.get('part_marker')
        next_marker = page.get('next_part_marker')
        if marker is not None and not isinstance(marker, str):
            raise VerificationError('part_marker 必须是字符串或 null')
        if next_marker is not None and not isinstance(next_marker, str):
            raise VerificationError('next_part_marker 必须是字符串或 null')
        markers.append(marker)
        nexts.append(next_marker)
        entry_count += len(page['parts'])
    if len(set(markers)) != len(markers):
        raise VerificationError('part 页 marker 重复，存在重复分页')
    marker_set = set(markers)
    terminal = [m for m, nxt in zip(markers, nexts) if nxt is None]
    if len(terminal) != 1:
        raise VerificationError(f'part 页必须恰有一个末页，实际 {len(terminal)} 个')
    for nxt in nexts:
        if nxt is not None and nxt not in marker_set:
            raise VerificationError(f'next_part_marker {nxt!r} 指向未取到的页，分页不完整')
    referenced = {nxt for nxt in nexts if nxt is not None}
    starts = marker_set - referenced
    if len(starts) != 1:
        raise VerificationError('part 分页链存在多个起点或成环')
    if entry_count != part_count:
        raise VerificationError('part 页条目数与去重后的 part 数不符')


def _normalise_pages(pages):
    """把可能乱序、重复或缺页的 part 页整理成严格连续的 part 序列。"""
    if not isinstance(pages, (list, tuple)) or not pages:
        raise VerificationError('COMPOSITE 对象缺少 part 元数据页')
    collected = {}
    for page in pages:
        if not isinstance(page, dict):
            raise VerificationError('part 页必须为对象')
        parts = page.get('parts')
        if not isinstance(parts, list):
            raise VerificationError('part 页缺少 parts 数组')
        for part in parts:
            if not isinstance(part, dict):
                raise VerificationError('part 条目必须为对象')
            number = part.get('number')
            if type(number) is not int or number < 1:
                raise VerificationError('part number 必须是从 1 开始的正整数')
            if number in collected:
                raise VerificationError(f'part {number} 在分页中重复出现')
            collected[number] = part
    numbers = sorted(collected)
    if numbers != list(range(1, len(numbers) + 1)):
        raise VerificationError(
            f'part 序号不连续: 收到 {numbers}，应为 1..{len(numbers)}')
    ordered = [collected[n] for n in numbers]
    _audit_page_chain(pages, len(ordered))
    return ordered


def verify_composite(content, declared_checksum, pages, declared_size=None):
    """组合对象：按服务 part 边界逐段核对，再校验组合摘要与 part 总数。

    pages 是 GetObjectAttributes 各 marker 页的有序返回列表（页本身可乱序）。
    content 是固定 versionId GET 到的完整对象字节。
    """
    parts = _normalise_pages(pages)

    offset = 0
    part_digests = []
    for part in parts:
        number = part['number']
        size = part.get('size')
        if type(size) is not int or size < 0:
            raise VerificationError(f'part {number} size 必须是非负整数')
        end = offset + size
        if end > len(content):
            raise VerificationError(
                f'part {number} 边界 {end} 超出对象长度 {len(content)}')
        segment = content[offset:end]
        if len(segment) != size:
            raise VerificationError(f'part {number} 实际边界长度与声明 size 不符')
        digest = hashlib.sha256(segment).digest()
        declared_hex = part.get('sha256')
        if isinstance(declared_hex, str) and declared_hex:
            if digest.hex() != declared_hex.lower():
                raise VerificationError(f'part {number} SHA-256 与服务声明不符')
        declared_b64 = part.get('checksum')
        if declared_b64 is not None and _b64_digest(
                declared_b64, f'part {number} checksum') != digest:
            raise VerificationError(f'part {number} checksum 与该段内容不符')
        part_digests.append(digest)
        offset = end

    if offset != len(content):
        raise VerificationError(
            f'服务 part 边界总长 {offset} 不等于对象实际长度 {len(content)}')
    if declared_size is not None and offset != declared_size:
        raise VerificationError('对象长度与清单/服务声明的 size 不符')

    if not isinstance(declared_checksum, str) or '-' not in declared_checksum:
        raise VerificationError('COMPOSITE checksum 必须是 "<base64>-<part 数>" 形式')
    b64_part, _, count_text = declared_checksum.rpartition('-')
    if not count_text.isdigit():
        raise VerificationError('COMPOSITE checksum 的 part 总数不是整数')
    if int(count_text) != len(parts):
        raise VerificationError(
            f'COMPOSITE part 总数不符: 声明 {count_text}，实际取到 {len(parts)}')
    composite = hashlib.sha256(b''.join(part_digests)).digest()
    if _b64_digest(b64_part, 'COMPOSITE checksum') != composite:
        raise VerificationError('COMPOSITE 组合 SHA-256 与各 part 摘要拼接结果不符')
    return {
        # 全文内容 SHA：始终对完整对象字节计算，与组合值是两个不同概念。
        'full_content_sha256': hashlib.sha256(content).hexdigest(),
        'composite_sha256': composite.hex(),
        'part_count': len(parts),
        'part_sha256': [d.hex() for d in part_digests],
    }


def verify_object(content, spec, pages):
    """按对象声明的 checksum_type 分派校验，返回可写入收据的结论。"""
    checksum_type = spec.get('checksum_type')
    if checksum_type == _FULL:
        return {
            'checksum_type': _FULL,
            'sha256': verify_full_object(
                content, spec['checksum'], spec.get('sha256')),
        }
    if checksum_type == _COMPOSITE:
        detail = verify_composite(content, spec['checksum'], pages, spec.get('size'))
        return {
            'checksum_type': _COMPOSITE,
            'sha256': detail['full_content_sha256'],
            'composite_sha256': detail['composite_sha256'],
            'part_count': detail['part_count'],
            'part_sha256': detail['part_sha256'],
        }
    raise VerificationError(f'不支持的 checksum_type: {checksum_type!r}')
