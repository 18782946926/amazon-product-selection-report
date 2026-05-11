from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from llm.client import LLMClient
from llm.exceptions import LLMSchemaError, LLMUnavailable

log = logging.getLogger(__name__)

P = TypeVar("P", bound=BaseModel)


class BaseAnalyzer(ABC, Generic[P]):
    """所有 analyzer 的基类。run() 编排：缓存 → LLM → 降级。"""

    name: str = "base"

    def __init__(self, client: LLMClient | None = None):
        self.client = client

    @abstractmethod
    def _cache_key(self, input_data: Any) -> str:
        ...

    @abstractmethod
    def _call_llm(self, input_data: Any) -> P:
        """调用 LLM 产出 Pack。失败抛 LLMUnavailable / LLMSchemaError。"""

    @abstractmethod
    def _fallback(self, input_data: Any) -> P:
        """LLM 不可用时的硬编码统计降级 Pack。"""

    def run(self, input_data: Any) -> P:
        if self.client is None:
            log.warning("[%s] 无 LLM client，直接降级", self.name)
            return self._fallback(input_data)

        try:
            pack = self._call_llm(input_data)
            log.info("[%s] LLM 调用成功", self.name)
            return pack
        except (LLMUnavailable, LLMSchemaError) as e:
            log.warning("[%s] LLM 失败，走降级: %s", self.name, e)
            fb = self._fallback(input_data)
            if hasattr(fb, "is_fallback"):
                fb.is_fallback = True
            return fb
        except Exception as e:
            log.error("[%s] 未预期异常，走降级: %s", self.name, e, exc_info=True)
            fb = self._fallback(input_data)
            if hasattr(fb, "is_fallback"):
                fb.is_fallback = True
            return fb
