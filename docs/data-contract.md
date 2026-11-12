# 封存对象清单

schema给出CSV列顺序，分片没有额外表头。键使用百分号编码，+是字面字符；version_id不解码。is_latest/delete_marker只能为true或false。null版本是字面字符串null，缺version_id列或空身份应报资料不完整。清单分片先按声明的bytes和sha256核对再解压解析。

对象适配器操作为GetObject(bucket,key,version_id)、GetObjectAttributes(...,part_marker)，后者按marker返回part页；页可能乱序，应按part number核对连续、唯一及完整分页。FULL_OBJECT是Base64(SHA256完整对象)，COMPOSITE是Base64(SHA256按序连接的各part原始32字节摘要))加连字符与part总数。part的长度和SHA256分别核对，不能用ETag代替此链。组合型只有全部part元数据完整才可正式验证。

本地对象路径应由独立内部身份映射产生，原键可含/、+、空格或点段，不直接作为文件路径。清单是封存资料而非ListObjectVersions的跨页瞬时快照。domain只清点解压后的CSV行，不取版本、不验证对象校验链。

## 版本采集口径（collector/checksum/source）

身份三元组 bucket、key、version_id 中，version_id 是不解释字符串；字面 `null` 表示 null 版本 ID，缺列或空字符串一律按资料不完整拒绝。当前版视图仅采用 `is_latest=true` 且非墓碑的行；`delete_marker=true` 只把状态记为 tombstone，不发起 GetObject。是否收集其余历史版由 manifest 的 `collect_history` 决定。同一身份重复出现但 is_latest/delete_marker/size 不一致，或同一 (bucket,key) 出现多条 latest，都属于清单冲突，必须报错。

GetObject 与 GetObjectAttributes 的每次请求都显式携带固定 versionId；响应必须回显同一 bucket/key/version_id，回显其他版本即视为源忽略了版本参数，按 SourceProtocolError 拒绝且不发布任何文件。响应 size 必须与清单 size、实际字节数三者一致。ETag 仅写入收据的 object.etag 作为辅助身份，带短横线的 ETag 不是合法 MD5，校验链从不读取它。

FULL_OBJECT 的 checksum 是 Base64(SHA256(完整对象字节))，可附带同义的十六进制 sha256 交叉核对。COMPOSITE 的 checksum 形如 `Base64(SHA256(concat(各 part 的 32 字节原始摘要)))-<part 总数>`。part 页通过 part_marker/next_part_marker 翻取，页到达顺序允许乱序：采集器按 part number 排序并要求序号从 1 连续、唯一，同时用 marker 集合审计恰好一个起点、一个末页、无悬空 next、无重复页。每个 part 按服务声明的 size 切出原文边界，核对其十六进制 sha256 与 Base64 checksum；part 边界总长必须等于对象长度。只有全部 part 元数据完整且组合值、part 总数同时通过，COMPOSITE 才算验证通过。禁止用重新按本地分块大小算出的摘要替代上述服务分片校验。

发布与恢复：数据文件与 `<hash>.receipt.json` 都经同目录临时文件 + os.replace 原子落盘，先数据后收据再翻转 collect-state.json 中的状态；状态取值为 manifest/inflight/verified/failed/tombstone。收据记录 manifest generation、分片 key 与物理行号、服务回显身份、全文 content_sha256、校验链细节与本地相对路径，使每个最终文件都能逐一回链。重跑时 verified 条目只凭收据与磁盘字节复核（不重新联系源），复核不过则回退状态并重新采集；状态文件 generation 与当前清单不符时拒绝启动。清单之外出现的任何新版本都不在本次采集范围内，也不得据此声称清单等于实时桶状态。
