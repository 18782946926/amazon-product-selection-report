from __future__ import annotations

from typing import Literal

from llm.exceptions import LLMUnavailable
from llm.providers.base import BaseProvider, ModelTier


class DeepSeekProvider(BaseProvider):
    name = "deepseek"

    def __init__(self, api_key: str, base_url: str, fast_model: str, smart_model: str):
        if not api_key:
            raise LLMUnavailable("DEEPSEEK_API_KEY 未配置")
        try:
            from openai import OpenAI
        except ImportError as e:
            raise LLMUnavailable(f"openai 库未安装: {e}")
        self._client = OpenAI(api_key=api_key, base_url=base_url)
        self.fast_model = fast_model
        self.smart_model = smart_model

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
            raise LLMUnavailable(f"DeepSeek 调用失败: {e}") from e
        if not resp.choices or not resp.choices[0].message.content:
            raise LLMUnavailable("DeepSeek 返回空内容")
        return resp.choices[0].message.content
