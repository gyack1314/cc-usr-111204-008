"""版本采集各阶段的明确失败类型。"""


class ManifestError(ValueError):
    """封存清单本身不合法：分片核对、schema、行字段或身份冲突。"""


class SourceProtocolError(RuntimeError):
    """适配器违反源协议：未携带/未尊重版本参数、分页或响应字段缺失。"""


class VerificationError(RuntimeError):
    """对象字节或校验链与清单/服务声明不一致。"""
