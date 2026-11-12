# 试验对象存储的版本清单与组合校验采集

此Python项目管理独立的能源领域合成源资料，`data/`为原始样本及访问配置，`source_data.io`负责严格读取和字节身份，`source_data.domain`提供现有库存摘要，`source_data.replay`按声明顺序返回不解释的源响应。字段和支持的来源口径见[资料约定](docs/data-contract.md)。所有机构、设备、对象与数值均为虚构。

Python 3.11及以上即可使用，无第三方Python依赖。项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q source_data tests
python -m source_data.cli inspect data/normal.json
```

`inspect`展示已有的原始库存，不修改资料。`invalid-`前缀文件为不合法输入，其他变体保留不同业务边界；源协议与二进制格式使用资料约定中的有限范围，未解释部分仍以原字节保存。

## 以封存清单为依据的版本采集

`source_data.manifest`先按 manifest 声明逐分片核对 gzip 字节长度与 SHA-256，再以`GzipFile`流式逐行读取 CSV，列顺序完全由 schema 决定。键只做一次百分号解码（字面`+`不是空格），`version_id`是不解释字符串，字面`null`与缺字段/空值严格区分。身份三元组为 bucket/key/version_id；当前版视图只取`is_latest=true`，`delete_marker=true`只记墓碑不发起 GET。重复身份元数据不一致或同 key 多条 latest 一律报冲突。

`source_data.collector`对每个对象固定`versionId`发起请求，并校验响应回显的版本、长度与元数据；拒绝忽略版本参数而返回当前内容的源。ETag 只作身份辅助记录，绝不直接充当完整内容 MD5。校验链在`source_data.checksum`：

- `FULL_OBJECT`：对全文字节算一次 SHA-256，与服务 Base64 声明直接比对；
- `COMPOSITE`：通过`GetObjectAttributes`按 marker 翻完 part 页（页可乱序，按 part number 重建连续、唯一、完整的序列），逐 part 按**服务声明的边界**核对长度与 SHA-256，再校验由各 part 原始 32 字节摘要按序拼接得到的组合值及 part 总数。不会用本地自定分块大小重算的值替代服务分片校验。

下载可并行，但清单身份（manifest）、进行中对象（inflight）、已核验对象（verified）分三段状态持久化。只有完整校验通过后才以同目录临时文件加`os.replace`原子发布数据文件与版本收据；崩溃后已发布对象按收据与磁盘字节复核复用，孤立临时文件不会被当作成功。本地路径由内部身份的 SHA-256 映射产生，源键不直接成为相对路径，同 key 不同版本不会碰撞，点段/斜杠键也无法越界。每份收据都回链到清单分片行号与服务版本。清单外的运行时新版本不纳入采集，清单是封存资料而非实时桶快照。

```sh
python -m source_data.cli collect data/normal.json out/                 # 含历史版
python -m source_data.cli collect data/normal.json out/ --current-only  # 仅当前版
python -m source_data.cli collect data/normal.json out/ --page-order reverse
python -m source_data.cli collect data/mismatched-part.json out/        # 组合失配，退出码 2
```

固定基础镜像见Dockerfile，可执行`docker build -t energy-source .`，再用`docker run --rm energy-source`显示摘要；`docker run --rm energy-source python -m unittest discover -s tests -v`执行已有检查。资料和回放不依赖真实设备、账户或实时网络服务。
