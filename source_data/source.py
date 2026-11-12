"""离线对象适配器：把封存资料中的对象字节与回放脚本暴露为版本固定的源。

不发起任何网络请求。两种用法：
- ReplayObjectSource：按 manifest objects 声明直接应答，可注入乱序 part 页、
  忽略版本参数等故障，用于复演。
- ScriptedReplaySource：严格按声明的操作/参数顺序消费 replay.Replay，
  用于断言采集器发出的每个请求都固定了 versionId。
"""
from .errors import SourceProtocolError
from .manifest import Identity
from .replay import Replay


def _find_spec(objects, ident: Identity):
    for spec in objects:
        if spec.get('bucket') == ident.bucket and spec.get('key') == ident.key \
                and spec.get('version_id') == ident.version_id:
            return spec
    raise SourceProtocolError(
        f'清单身份在源声明中无对应对象: {ident.bucket}/{ident.key} '
        f'版本 {ident.version_id}')


class ReplayObjectSource:
    """由 manifest JSON 的 objects 段支撑的内存适配器。"""

    def __init__(self, manifest_value, data_root, fault=None):
        self.objects = manifest_value['objects']
        self.data_root = data_root
        # fault 可包含: ignore_version（回显其他版本）、page_order（part 页置换）、
        # missing_page（丢一页）、bad_etag_as_md5 仅作记录不影响校验。
        self.fault = fault or {}
        self.request_log = []
        # 每个 COMPOSITE 身份预生成一份可能乱序/缺页的页计划：marker 链始终
        # 指向逻辑下一页，但计划内页的存放顺序决定采集器实际收到的顺序。
        self._page_plans = {
            self._ident_of(spec): self._build_plan(spec)
            for spec in self.objects
            if spec.get('checksum_type') == 'COMPOSITE'
        }
        self._plan_cursor = {}

    @staticmethod
    def _ident_of(spec):
        return Identity(spec['bucket'], spec['key'], spec['version_id']).token()

    def spec_for(self, ident: Identity):
        return _find_spec(self.objects, ident)

    def _load_body(self, spec):
        path = self.data_root / spec['file']
        return path.read_bytes()

    def get_object(self, bucket, key, version_id):
        self.request_log.append(
            {'operation': 'GetObject', 'bucket': bucket, 'key': key,
             'version_id': version_id})
        ident = Identity(bucket, key, version_id)
        spec = _find_spec(self.objects, ident)
        body = self._load_body(spec)
        echo_version = version_id
        if self.fault.get('ignore_version'):
            # 模拟忽略 versionId、回当前版内容的源：回显身份必须对不上。
            others = [o for o in self.objects
                      if o['bucket'] == bucket and o['key'] == key
                      and o['version_id'] != version_id]
            if others:
                spec = others[0]
                body = self._load_body(spec)
                echo_version = spec['version_id']
        etag = spec.get('etag', '"' + 'd41d8cd98f00b204e9800998ecf8427e"')
        if self.fault.get('dash_etag'):
            etag = '"' + 'a-b-c-d-e-f-g-h-i-j-k-l-m-n-o-p-q-r-s-t-u-v"'
        return {
            'bucket': bucket,
            'key': key,
            'version_id': echo_version,
            'size': len(body),
            'etag': etag,
            'body': body,
        }

    def _logical_pages(self, spec):
        """每个 part 一页的基准页，marker 链按 part number 顺序连接。"""
        pages = []
        marker = None
        for index, part in enumerate(spec['parts']):
            pages.append({
                'bucket': spec['bucket'],
                'key': spec['key'],
                'version_id': spec['version_id'],
                'part_marker': marker,
                'next_part_marker': (f'm{index + 1}'
                                     if index + 1 < len(spec['parts']) else None),
                'parts': [dict(part)],
            })
            marker = f'm{index + 1}'
        return pages

    def _build_plan(self, spec):
        """按故障配置生成服务顺序的页计划。

        页可乱序：part number 到达顺序被打乱，但每页的 marker/next 按实际
        服务顺序重链，保证协议自洽；缺页则保留悬空 next 制造不完整分页。
        """
        pages = self._logical_pages(spec)
        order = self.fault.get('page_order')
        if order == 'reverse' and len(pages) > 1:
            ordered = list(reversed(pages))
        elif order == 'swap_first_two' and len(pages) > 1:
            ordered = [pages[1], pages[0]] + pages[2:]
        else:
            ordered = pages
        plan = []
        for index, page in enumerate(ordered):
            plan.append({
                **page,
                'part_marker': None if index == 0 else f'p{index}',
                'next_part_marker': (None if index == len(ordered) - 1
                                     else f'p{index + 1}'),
            })
        if self.fault.get('missing_page') and plan:
            # 末页摘除：上一页的 next 仍指向它，形成悬空引用。
            plan = plan[:-1]
        return plan

    def plan_for(self, ident: Identity):
        """测试辅助：暴露某 COMPOSITE 身份实际会被服务的页集合。"""
        plan = self._page_plans.get(ident.token())
        if plan is None:
            raise SourceProtocolError('该身份不是 COMPOSITE 对象')
        return [dict(page) for page in plan]

    def get_object_attributes(self, bucket, key, version_id, part_marker):
        self.request_log.append(
            {'operation': 'GetObjectAttributes', 'bucket': bucket, 'key': key,
             'version_id': version_id, 'part_marker': part_marker})
        ident = Identity(bucket, key, version_id)
        spec = _find_spec(self.objects, ident)
        if spec.get('checksum_type') != 'COMPOSITE':
            raise SourceProtocolError('非 COMPOSITE 对象没有 part 页')
        plan = self._page_plans[ident.token()]
        cursor = self._plan_cursor.setdefault(ident.token(), 0)
        if cursor >= len(plan):
            raise SourceProtocolError(
                f'part 页已耗尽却被继续请求: marker={part_marker!r}')
        page = plan[cursor]
        if page['part_marker'] != part_marker:
            raise SourceProtocolError(
                f"part_marker 顺序不符: 期望 {page['part_marker']!r}，"
                f'收到 {part_marker!r}')
        self._plan_cursor[ident.token()] = cursor + 1
        echo = (page['bucket'], page['key'], page['version_id'])
        if echo != (bucket, key, version_id):
            raise SourceProtocolError('part 页未回显请求的固定版本身份')
        return dict(page)


class ScriptedReplaySource:
    """严格脚本源：每个请求的参数（含固定 versionId）必须与脚本逐字相符。"""

    def __init__(self, manifest_value, steps):
        self.objects = manifest_value['objects']
        self.replay = Replay(steps)

    def spec_for(self, ident: Identity):
        return _find_spec(self.objects, ident)

    def get_object(self, bucket, key, version_id):
        return self.replay.call('GetObject', {
            'bucket': bucket, 'key': key, 'version_id': version_id})

    def get_object_attributes(self, bucket, key, version_id, part_marker):
        return self.replay.call('GetObjectAttributes', {
            'bucket': bucket, 'key': key, 'version_id': version_id,
            'part_marker': part_marker})
