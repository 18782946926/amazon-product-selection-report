"""LLM 模块：统一客户端、Provider、缓存、Analyzer。

主要入口：
- LLMClient: 统一调用入口
- make_client(): 工厂函数，按环境变量构建 Provider 和 Cache
"""
from llm.client import LLMClient, make_client
from llm.exceptions import LLMUnavailable, LLMSchemaError

__all__ = ["LLMClient", "make_client", "LLMUnavailable", "LLMSchemaError"]
