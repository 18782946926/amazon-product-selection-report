class LLMError(Exception):
    pass


class LLMUnavailable(LLMError):
    """LLM API 不可用：网络故障、超时、key 错误、限流等。调用方应触发降级。"""


class LLMSchemaError(LLMError):
    """LLM 返回内容无法通过 pydantic schema 校验。调用方应触发降级。"""
