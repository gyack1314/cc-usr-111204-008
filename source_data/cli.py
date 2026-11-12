"""显示资料库存与原始字段摘要，并按封存清单执行版本采集。"""
import argparse
import json
from pathlib import Path

from .collector import VersionedCollector
from .domain import summarize
from .io import load
from .source import ReplayObjectSource


def _load(args):
    value, digest = load(args.file)
    return value, digest, Path(args.file).resolve().parent


def main():
    p = argparse.ArgumentParser(
        description='封存清单驱动的对象版本采集工具（离线回放）')
    sub = p.add_subparsers(dest='command', required=True)

    p_inspect = sub.add_parser('inspect', help='展示已有原始库存，不修改资料')
    p_inspect.add_argument('file')

    p_collect = sub.add_parser('collect', help='按清单身份采集固定版本对象')
    p_collect.add_argument('file', help='manifest JSON 资料路径')
    p_collect.add_argument('out', help='采集输出目录（按内部身份映射）')
    p_collect.add_argument('--current-only', action='store_true',
                           help='只采集 is_latest=true 的非墓碑版本')
    p_collect.add_argument('--page-order', choices=['reverse', 'swap_first_two'],
                           help='向回放源注入 part 页乱序')
    p_collect.add_argument('--missing-page', action='store_true',
                           help='向回放源注入 part 缺页')
    p_collect.add_argument('--ignore-version', action='store_true',
                           help='模拟忽略 versionId 而回当前内容的源')
    p_collect.add_argument('--dash-etag', action='store_true',
                           help='让源回带短横线的 ETag（不得被当作 MD5）')

    args = p.parse_args()
    value, digest, data_root = _load(args)

    if args.command == 'inspect':
        print(json.dumps(
            {'source_sha256': digest, 'summary': summarize(value, data_root)},
            ensure_ascii=False, indent=2))
        return

    fault = {}
    if args.page_order:
        fault['page_order'] = args.page_order
    if args.missing_page:
        fault['missing_page'] = True
    if args.ignore_version:
        fault['ignore_version'] = True
    if args.dash_etag:
        fault['dash_etag'] = True

    source = ReplayObjectSource(value, data_root, fault=fault)
    collector = VersionedCollector(
        value, data_root, Path(args.out), source,
        max_parallel=value.get('max_parallel', 2))
    report = collector.run(collect_history=not args.current_only)
    report['manifest_sha256'] = digest
    report['source_requests'] = list(source.request_log)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report['failures']:
        raise SystemExit(2)


if __name__ == '__main__':
    main()
