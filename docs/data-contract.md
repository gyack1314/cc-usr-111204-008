# 封存对象清单

schema给出CSV列顺序，分片没有额外表头。键使用百分号编码，+是字面字符；version_id不解码。is_latest/delete_marker只能为true或false。null版本是字面字符串null，缺version_id列或空身份应报资料不完整。清单分片先按声明的bytes和sha256核对再解压解析。

对象适配器操作为GetObject(bucket,key,version_id)、GetObjectAttributes(...,part_marker)，后者按marker返回part页；页可能乱序，应按part number核对连续、唯一及完整分页。FULL_OBJECT是Base64(SHA256完整对象)，COMPOSITE是Base64(SHA256按序连接的各part原始32字节摘要))加连字符与part总数。part的长度和SHA256分别核对，不能用ETag代替此链。组合型只有全部part元数据完整才可正式验证。

本地对象路径应由独立内部身份映射产生，原键可含/、+、空格或点段，不直接作为文件路径。清单是封存资料而非ListObjectVersions的跨页瞬时快照。domain只清点解压后的CSV行，不取版本、不验证对象校验链。
