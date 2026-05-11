from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal

ModelTier = Literal["fast", "smart"]


class BaseProvider(ABC):
    name: str = "base"
    fast_model: str = ""
    smart_model: str = ""
    vision_model: str = ""

    @abstractmethod
    def chat(
        self,
        messages: list[dict],
        *,
        tier: ModelTier = "fast",
        response_format: Literal["text", "json"] = "text",
        temperature: float = 0.3,
        max_tokens: int = 4096,
        timeout: float = 60.0,
    ) -> str:
        """发起一次同步 chat 调用，返回纯文本（json 模式下也是 JSON 字符串）。

        失败时应抛 llm.exceptions.LLMUnavailable。
        """

    def chat_multimodal(
        self,
        messages: list[dict],
        *,
        response_format: Literal["text", "json"] = "text",
        temperature: float = 0.0,
        max_tokens: int = 1024,
        timeout: float = 60.0,
    ) -> str:
        """发起一次多模态 chat（图+文）。messages 的 content 为 OpenAI 格式数组：
        [{"type": "text", "text": "..."}, {"type": "image_url", "image_url": {"url": "https://..."}}]

        默认实现抛 NotImplementedError；不支持视觉的 provider 可保持默认。
        判断是否支持请用 supports_vision()。
        """
        raise NotImplementedError(f"{self.name} provider 不支持多模态调用")

    def supports_vision(self) -> bool:
        """是否支持多模态调用。默认 False。"""
        return False

    def model_for(self, tier: ModelTier) -> str:
        return self.smart_model if tier == "smart" else self.fast_model
