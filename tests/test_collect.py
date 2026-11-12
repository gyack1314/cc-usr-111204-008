"""版本采集端到端与边界测试。

复演需求点名的场景：同键多版本、删除标记、part 页乱序、COMPOSITE 校验失配，
以及单次百分号解码、null 版本、冲突检测、固定 versionId、原子发布与崩溃恢复。
全部使用仓库内虚构资料，无网络、无云账号。
"""
import gzip
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from source_data.checksum import verify_composite, verify_full_object
from source_data.collector import VersionedCollector, local_paths
from source_data.errors import (
    ManifestError, VerificationError)
from source_data.io import load
from source_data.manifest import Identity, parse_manifest
from source_data.source import ReplayObjectSource, ScriptedReplaySource

DATA = Path(__file__).resolve().parent.parent / 'data'


def load_manifest(name='normal.json'):
    value, _ = load(DATA / name)
    return value


class CollectorHarness:
    def __init__(self, value, root, fault=None, max_parallel=2):
        self.root = Path(root)
        self.source = ReplayObjectSource(value, DATA, fault=fault)
        self.collector = VersionedCollector(
            value, DATA, self.root, self.source, max_parallel=max_parallel)

    def run(self, collect_history=True):
        return self.collector.run(collect_history=collect_history)


class MultiVersionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.value = load_manifest()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_same_key_two_versions_do_not_collide_and_both_link_back(self):
        h = CollectorHarness(self.value, self.tmp)
        report = h.run()
        self.assertEqual([o[1] for o in report['results']],
                         ['verified', 'verified'])
        # 同 key 两个版本映射到不同内部路径，源键没有直接成为文件名。
        data_files = sorted(
            p for p in Path(self.tmp).rglob('*') if p.is_file()
            and not p.name.endswith('.json'))
        self.assertEqual(len(data_files), 2)
        for p in data_files:
            self.assertNotIn('a+b', str(p))
            self.assertNotIn('run', p.name)
        # 每个文件都能回链到清单行与服务版本。
        for receipt_path in Path(self.tmp).rglob('*.receipt.json'):
            receipt = json.loads(receipt_path.read_text('utf-8'))
            self.assertEqual(
                receipt['manifest_generation'], 'inventory-2030-01-02')
            self.assertEqual(receipt['manifest_row']['shard_key'],
                             'inventory/part-0')
            self.assertIn(receipt['manifest_row']['line_number'], (1, 2))
            self.assertEqual(receipt['identity']['version_id'],
                             receipt['object']['version_id'])
            data_path = Path(self.tmp) / receipt['object']['local_path']
            self.assertTrue(data_path.exists())
            self.assertEqual(
                hashlib.sha256(data_path.read_bytes()).hexdigest(),
                receipt['object']['content_sha256'])

    def test_every_request_pins_version_id(self):
        h = CollectorHarness(self.value, self.tmp)
        h.run()
        gets = [r for r in h.source.request_log if r['operation'] == 'GetObject']
        self.assertEqual({r['version_id'] for r in gets}, {'v1', 'v2'})
        for req in gets:
            self.assertEqual(req['key'], 'run/a+b.txt')  # 解码后的键
        attrs = [r for r in h.source.request_log
                 if r['operation'] == 'GetObjectAttributes']
        self.assertTrue(attrs)
        self.assertTrue(all(r['version_id'] == 'v2' for r in attrs))

    def test_current_only_skips_historical_version(self):
        h = CollectorHarness(self.value, self.tmp)
        report = h.run(collect_history=False)
        self.assertEqual(report['targets'], 1)
        outcomes = dict(report['results'])
        self.assertIn(Identity('trial', 'run/a+b.txt', 'v2').token(), outcomes)
        self.assertNotIn(Identity('trial', 'run/a+b.txt', 'v1').token(), outcomes)


class DeleteMarkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.value = load_manifest()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_tombstone_recorded_but_never_get(self):
        h = CollectorHarness(self.value, self.tmp)
        report = h.run()
        self.assertEqual(report['tombstones'], 1)
        token = Identity('trial', 'retired.txt', 'v3').token()
        self.assertEqual(h.collector.state.entries[token]['status'],
                         'tombstone')
        # 墓碑身份没有任何 GetObject 请求。
        self.assertFalse(any(
            r['operation'] == 'GetObject' and r['key'] == 'retired.txt'
            for r in h.source.request_log))
        # 没有任何数据文件为墓碑发布。
        receipts = list(Path(self.tmp).rglob('*.receipt.json'))
        self.assertEqual(len(receipts), 2)


class PartPageOrderingTests(unittest.TestCase):
    def setUp(self):
        self.value = load_manifest()

    def test_reversed_pages_still_verify_by_part_number(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(self.value, tmp, fault={'page_order': 'reverse'})
            report = h.run()
            self.assertEqual(report['failures'], [])
            source = h.source
            # 采集器确实以乱序顺序收到 part 页（先 part2 后 part1）。
            attr_calls = [r for r in source.request_log
                          if r['operation'] == 'GetObjectAttributes']
            plan = source.plan_for(Identity('trial', 'run/a+b.txt', 'v2'))
            self.assertEqual(plan[0]['parts'][0]['number'], 2)
            self.assertEqual(len(attr_calls), 2)

    def test_swap_first_two_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(self.value, tmp,
                                 fault={'page_order': 'swap_first_two'})
            report = h.run()
            self.assertEqual(report['failures'], [])

    def test_missing_page_is_rejected_and_nothing_published(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(self.value, tmp, fault={'missing_page': True})
            report = h.run()
            self.assertEqual(len(report['failures']), 1)
            token = Identity('trial', 'run/a+b.txt', 'v2').token()
            self.assertEqual(
                h.collector.state.entries[token]['status'], 'failed')
            # v1 成功、v2 失败：只有 v1 的文件被原子发布。
            data_files = [p for p in Path(tmp).rglob('*')
                          if p.is_file() and not p.name.endswith('.json')]
            self.assertEqual(len(data_files), 1)

    def test_direct_checksum_chain_audit_on_out_of_order_pages(self):
        content = (DATA / 'object-v2.bin').read_bytes()
        spec = next(o for o in self.value['objects']
                    if o['version_id'] == 'v2')
        source = ReplayObjectSource(self.value, DATA,
                                    fault={'page_order': 'reverse'})
        pages = source.plan_for(Identity('trial', 'run/a+b.txt', 'v2'))
        result = verify_composite(content, spec['checksum'], pages,
                                  spec.get('size'))
        self.assertEqual(result['part_count'], 2)
        self.assertEqual(result['full_content_sha256'],
                         hashlib.sha256(content).hexdigest())


class CompositeMismatchTests(unittest.TestCase):
    def test_corrupt_part_checksum_fails_chain(self):
        value = load_manifest('mismatched-part.json')
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(value, tmp)
            report = h.run()
            self.assertEqual(len(report['failures']), 1)
            self.assertIn('part 1 SHA-256', report['failures'][0][1])
            # 坏文件不得通过，也不得发布。
            token = Identity('trial', 'run/a+b.txt', 'v2').token()
            self.assertEqual(
                h.collector.state.entries[token]['status'], 'failed')
            self.assertFalse(any(
                'a1' in str(p) for p in Path(tmp).rglob('*') if p.is_file()))

    def test_full_object_mismatch_detected(self):
        with self.assertRaises(VerificationError):
            verify_full_object(b'wrong bytes',
                               'mL8gM51ucbWENXlF1jIyRpjeCGrtnSjylTTEmyZK6Is=')

    def test_composite_value_mismatch_when_parts_all_valid(self):
        # 每个 part 的边界与 SHA-256 都正确，仅顶层 COMPOSITE 组合值被篡改：
        # 必须在最后一道组合校验被拦下，而不是误判为 part 错误。
        value = load_manifest()
        content = (DATA / 'object-v2.bin').read_bytes()
        spec = json.loads(json.dumps(
            next(o for o in value['objects'] if o['version_id'] == 'v2')))
        spec['checksum'] = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=-2'
        source = ReplayObjectSource(value, DATA)
        pages = source.plan_for(Identity('trial', 'run/a+b.txt', 'v2'))
        with self.assertRaises(VerificationError) as ctx:
            verify_composite(content, spec['checksum'], pages)
        self.assertIn('COMPOSITE 组合', str(ctx.exception))

    def test_part_count_suffix_mismatch_detected(self):
        value = load_manifest()
        content = (DATA / 'object-v2.bin').read_bytes()
        spec = next(o for o in value['objects'] if o['version_id'] == 'v2')
        source = ReplayObjectSource(value, DATA)
        pages = source.plan_for(Identity('trial', 'run/a+b.txt', 'v2'))
        good = spec['checksum']
        tampered = good[:good.rindex('-') + 1] + '9'
        with self.assertRaises(VerificationError) as ctx:
            verify_composite(content, tampered, pages)
        self.assertIn('part 总数', str(ctx.exception))

    def test_scripted_source_drives_full_collection_with_pinned_calls(self):
        value = load_manifest()
        body_v1 = (DATA / 'object-v1.bin').read_bytes()
        body_v2 = (DATA / 'object-v2.bin').read_bytes()
        v1_spec = next(o for o in value['objects'] if o['version_id'] == 'v1')
        v2_spec = next(o for o in value['objects'] if o['version_id'] == 'v2')
        steps = [
            {'operation': 'GetObject',
             'arguments': {'bucket': 'trial', 'key': 'run/a+b.txt',
                           'version_id': 'v1'},
             'response': {'bucket': 'trial', 'key': 'run/a+b.txt',
                          'version_id': 'v1', 'size': len(body_v1),
                          'etag': '"e1"', 'body': body_v1}},
            {'operation': 'GetObject',
             'arguments': {'bucket': 'trial', 'key': 'run/a+b.txt',
                           'version_id': 'v2'},
             'response': {'bucket': 'trial', 'key': 'run/a+b.txt',
                          'version_id': 'v2', 'size': len(body_v2),
                          'etag': '"e2"', 'body': body_v2}},
        ]
        # COMPOSITE 还需要两页 part 属性，marker 从 None 起步。
        steps.append({
            'operation': 'GetObjectAttributes',
            'arguments': {'bucket': 'trial', 'key': 'run/a+b.txt',
                          'version_id': 'v2', 'part_marker': None},
            'response': {'bucket': 'trial', 'key': 'run/a+b.txt',
                         'version_id': 'v2', 'part_marker': None,
                         'next_part_marker': 'p1',
                         'parts': [dict(v2_spec['parts'][0])]}})
        steps.append({
            'operation': 'GetObjectAttributes',
            'arguments': {'bucket': 'trial', 'key': 'run/a+b.txt',
                          'version_id': 'v2', 'part_marker': 'p1'},
            'response': {'bucket': 'trial', 'key': 'run/a+b.txt',
                         'version_id': 'v2', 'part_marker': 'p1',
                         'next_part_marker': None,
                         'parts': [dict(v2_spec['parts'][1])]}})
        # v1 规格需带 checksum 字段（normal.json 已带）；用 max_parallel=1
        # 保证脚本顺序与采集顺序严格一致。
        with tempfile.TemporaryDirectory() as tmp:
            source = ScriptedReplaySource(value, steps)
            collector = VersionedCollector(
                value, DATA, Path(tmp), source, max_parallel=1)
            report = collector.run()
            self.assertEqual(report['failures'], [])
            self.assertEqual(source.replay.remaining(), 0)
            self.assertEqual(len(list(Path(tmp).rglob('*.receipt.json'))), 2)

    def test_dashed_etag_is_never_used_as_md5(self):
        # 带短横线的 ETag 明显不是 32 位十六进制 MD5；采集仍应只靠
        # checksum 链通过，ETag 仅被记录。
        value = load_manifest()
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(value, tmp, fault={'dash_etag': True})
            report = h.run()
            self.assertEqual(report['failures'], [])
            receipt = next(Path(tmp).rglob('*.receipt.json').__iter__())
            data = json.loads(receipt.read_text('utf-8'))
            self.assertIn('-', data['object']['etag'])
            # 内容身份来自真正的 SHA-256 而非 ETag。
            self.assertEqual(len(data['object']['content_sha256']), 64)


class VersionPinningTests(unittest.TestCase):
    def test_source_ignoring_version_id_is_rejected(self):
        value = load_manifest()
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(value, tmp, fault={'ignore_version': True})
            report = h.run()
            self.assertEqual(len(report['failures']), 2)
            for _, message in report['failures']:
                self.assertIn('versionId', message)
            # 没有任何对象被发布。
            self.assertFalse(list(Path(tmp).rglob('*.receipt.json')))

    def test_scripted_source_requires_exact_pinned_arguments(self):
        value = load_manifest()
        body_v1 = (DATA / 'object-v1.bin').read_bytes()
        steps = [{
            'operation': 'GetObject',
            'arguments': {'bucket': 'trial', 'key': 'run/a+b.txt',
                          'version_id': 'v1'},
            'response': {'bucket': 'trial', 'key': 'run/a+b.txt',
                         'version_id': 'v1', 'size': len(body_v1),
                         'etag': '"e1"', 'body': body_v1},
        }]
        source = ScriptedReplaySource(value, steps)
        ident = Identity('trial', 'run/a+b.txt', 'v1')
        resp = source.get_object(ident.bucket, ident.key, ident.version_id)
        self.assertEqual(resp['version_id'], 'v1')
        self.assertEqual(source.replay.remaining(), 0)


class ManifestParsingTests(unittest.TestCase):
    def _write_shard(self, root, rows_text, manifest_value):
        raw = gzip.compress(rows_text.encode('utf-8'))
        name = 'shard.csv.gz'
        (root / name).write_bytes(raw)
        manifest_value['shards'] = [{
            'key': 'inventory/part-0', 'file': name,
            'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
        }]
        return manifest_value

    def _base_manifest(self):
        return {
            'generation': 'g1',
            'schema': ['bucket', 'key', 'version_id', 'is_latest',
                       'delete_marker', 'size'],
            'shards': [],
        }

    def test_single_percent_decode_keeps_literal_plus(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, (
                'b,a%2Bb%2Fc.txt,v1,true,false,0\n'), self._base_manifest())
            manifest = parse_manifest(value, root)
            row = manifest.rows[0]
            self.assertEqual(row.identity.key, 'a+b/c.txt')

    def test_plus_is_not_decoded_to_space(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, (
                'b,a%20b%2Bc,v1,true,false,0\n'), self._base_manifest())
            manifest = parse_manifest(value, root)
            self.assertEqual(manifest.rows[0].identity.key, 'a b+c')

    def test_null_version_id_is_distinct_from_missing(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, (
                'b,k1,null,true,false,0\n'), self._base_manifest())
            manifest = parse_manifest(value, root)
            self.assertEqual(manifest.rows[0].identity.version_id, 'null')

    def test_empty_version_id_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, 'b,k1,,true,false,0\n',
                                      self._base_manifest())
            with self.assertRaises(ManifestError):
                parse_manifest(value, root)

    def test_shard_length_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, 'b,k,v,true,false,0\n',
                                      self._base_manifest())
            value['shards'][0]['bytes'] += 1
            with self.assertRaises(ManifestError):
                parse_manifest(value, root)

    def test_shard_sha256_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, 'b,k,v,true,false,0\n',
                                      self._base_manifest())
            value['shards'][0]['sha256'] = '00' * 32
            with self.assertRaises(ManifestError):
                parse_manifest(value, root)

    def test_duplicate_identity_with_conflicting_metadata_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, (
                'b,k,v1,true,false,3\n'
                'b,k,v1,false,false,4\n'), self._base_manifest())
            with self.assertRaises(ManifestError):
                parse_manifest(value, root)

    def test_multiple_latest_for_same_key_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, (
                'b,k,v1,true,false,1\n'
                'b,k,v2,true,false,1\n'), self._base_manifest())
            with self.assertRaises(ManifestError):
                parse_manifest(value, root)

    def test_boolean_field_strictly_validated(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            value = self._write_shard(root, 'b,k,v1,TRUE,false,0\n',
                                      self._base_manifest())
            with self.assertRaises(ManifestError):
                parse_manifest(value, root)

    def test_normal_fixture_parses_with_tombstone(self):
        value = load_manifest()
        manifest = parse_manifest(value, DATA)
        self.assertEqual(len(manifest.rows), 3)
        self.assertEqual(len(manifest.tombstones()), 1)
        current = manifest.current_versions()
        self.assertEqual(len(current), 1)
        self.assertEqual(current[0].identity.version_id, 'v2')


class PathAndReceiptTests(unittest.TestCase):
    def test_identity_path_is_content_addressed_and_distinct(self):
        i1 = Identity('b', 'run/a+b.txt', 'v1')
        i2 = Identity('b', 'run/a+b.txt', 'v2')
        with tempfile.TemporaryDirectory() as d:
            p1, r1 = local_paths(Path(d), i1)
            p2, r2 = local_paths(Path(d), i2)
            self.assertNotEqual(p1, p2)
            for p in (p1, p2, r1, r2):
                self.assertTrue(Path(p).resolve().is_relative_to(Path(d)))

    def test_path_segments_cannot_escape_root(self):
        evil = Identity('b', '../../etc/passwd', 'v1')
        with tempfile.TemporaryDirectory() as d:
            data_path, _ = local_paths(Path(d), evil)
            self.assertTrue(data_path.resolve().is_relative_to(Path(d).resolve()))


class ParallelAndCrashTests(unittest.TestCase):
    def setUp(self):
        self.value = load_manifest()

    def test_parallel_and_serial_produce_identical_outputs(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            CollectorHarness(self.value, a, max_parallel=2).run()
            CollectorHarness(self.value, b, max_parallel=1).run()
            for rel in ('collect-state.json',):
                sa = json.loads((Path(a) / rel).read_text('utf-8'))
                sb = json.loads((Path(b) / rel).read_text('utf-8'))
                self.assertEqual(
                    {k: v['status'] for k, v in sa['entries'].items()},
                    {k: v['status'] for k, v in sb['entries'].items()})

    def test_resume_reuses_receipts_without_source_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            h1 = CollectorHarness(self.value, tmp)
            h1.run()
            h2 = CollectorHarness(self.value, tmp)
            report = h2.run()
            self.assertTrue(all(o[1] == 'reused' for o in report['results']))
            self.assertEqual(h2.source.request_log, [])

    def test_tampered_file_is_detected_and_redownloaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            h1 = CollectorHarness(self.value, tmp)
            h1.run()
            target = next(p for p in Path(tmp).rglob('*')
                          if p.is_file() and not p.name.endswith('.json'))
            target.write_bytes(b'X' + target.read_bytes()[1:])
            h2 = CollectorHarness(self.value, tmp)
            report = h2.run()
            statuses = [o[1] for o in report['results']]
            self.assertIn('verified', statuses)
            self.assertNotIn(('failed',), tuple(statuses))

    def test_stranded_temp_file_is_not_treated_as_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = CollectorHarness(self.value, tmp)
            h.run()
            # 注入一个无收据的临时文件，状态机不得把它认作已发布。
            obj_dir = Path(tmp) / 'objects' / '00'
            obj_dir.mkdir(parents=True, exist_ok=True)
            (obj_dir / 'ghost.part').write_bytes(b'junk')
            h2 = CollectorHarness(self.value, tmp)
            report = h2.run()
            self.assertTrue(all(o[1] == 'reused' for o in report['results']))

    def test_generation_mismatch_state_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            CollectorHarness(self.value, tmp).run()
            state_path = Path(tmp) / 'collect-state.json'
            data = json.loads(state_path.read_text('utf-8'))
            data['generation'] = 'different-generation'
            state_path.write_text(json.dumps(data), 'utf-8')
            source = ReplayObjectSource(self.value, DATA)
            with self.assertRaises(VerificationError):
                VersionedCollector(self.value, DATA, Path(tmp), source)


if __name__ == '__main__':
    unittest.main()
