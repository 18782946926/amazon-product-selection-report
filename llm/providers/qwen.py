from __future__ import annotations

from typing import Literal

from llm.exceptions import LLMUnavailable
from llm.providers.base import BaseProvider, ModelTier


class QwenProvider(BaseProvider):
    """通义千问 (DashScope) Provider，使用 OpenAI 兼容 endpoint 简化调用。
    text + vision 同一个 client，按调用切 model。"""

    name = "qwen"

    def __init__(self, api_key: str, fast_model: str, smart_model: str, vision_model: str = "qwen-vl-max"):
        if not api_key:
            raise LLMUnavailable("DASHSCOPE_API_KEY 未配置")
        try:
            from openai import OpenAI
        except ImportError as e:
            raise LLMUnavailable(f"openai 库未安装: {e}")
        self._client = OpenAI(
            api_key=api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        self.fast_model = fast_model
        self.smart_model = smart_model
        self.vision_model = vision_model

    def supports_vision(self) -> bool:
        return True

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
        kwargs = {
            "model": self.model_for(tier),
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "timeout": timeout,
        }
        if response_format == "json":
            kwargs["response_format"] = {"type": "json_object"}
        try:
            resp = self._client.chat.completions.create(**kwargs)
        except Exception as e:
            raise LLMUnavailable(f"Qwen 调用失败: {e}") from e
        if not resp.choices or not resp.choices[0].message.content:
            raise LLMUnavailable("Qwen 返回空内容")
        return resp.choices[0].message.content

    def chat_multimodal(
        self,
        messages: list[dict],
        *,
        response_format: Literal["text", "json"] = "text",
        temperature: float = 0.0,
        max_tokens: int = 1024,
        timeout: float = 60.0,
    ) -> str:
        kwargs = {
            "model": self.vision_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "timeout": timeout,
        }
        if response_format == "json":
            kwargs["response_format"] = {"type": "json_object"}
        try:
            resp = self._client.chat.completions.create(**kwargs)
        except Exception as e:
            raise LLMUnavailable(f"Qwen 视觉调用失败: {e}") from e
        if not resp.choices or not resp.choices[0].message.content:
            raise LLMUnavailable("Qwen 视觉返回空内容")
        return resp.choices[0].message.content
