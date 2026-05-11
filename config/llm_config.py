from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from llm.exceptions import LLMUnavailable
from llm.providers.base import BaseProvider


@dataclass
class LLMSettings:
    provider: str = "qwen"
    timeout_seconds: float = 60.0
    max_retries: int = 1
    cache_dir: str = "llm_cache"


def load_settings() -> LLMSettings:
    return LLMSettings(
        provider=os.getenv("LLM_PROVIDER", "qwen").lower().strip(),
        timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "60")),
        max_retries=int(os.getenv("LLM_MAX_RETRIES", "1")),
        cache_dir=os.getenv("LLM_CACHE_DIR", "llm_cache"),
    )


def make_provider(name: str | None = None, api_key: str | None = None) -> BaseProvider:
    """根据 LLM_PROVIDER 环境变量构建对应 Provider。
    全栈默认 Qwen（DashScope 通义千问）：文本 + 视觉一体，QWEN_VISION_MODEL 默认 qwen-vl-max。
    DeepSeek 已停用（公共 API 不支持视觉）。
    api_key 显式传入时覆盖环境变量（key pool 多窗口并发场景用）。"""
    name = (name or os.getenv("LLM_PROVIDER", "qwen")).lower().strip()

    if name == "qwen":
        from llm.providers.qwen import QwenProvider
        return QwenProvider(
            api_key=api_key or os.getenv("DASHSCOPE_API_KEY", ""),
            fast_model=os.getenv("QWEN_FAST_MODEL", "qwen-plus"),
            smart_model=os.getenv("QWEN_SMART_MODEL", "qwen-max"),
            vision_model=os.getenv("QWEN_VISION_MODEL", "qwen-vl-max"),
        )

    if name == "doubao":
        from llm.providers.doubao import DoubaoProvider
        return DoubaoProvider(
            api_key=api_key or os.getenv("ARK_API_KEY", ""),
            fast_model=os.getenv("DOUBAO_FAST_MODEL", "doubao-1-5-lite-32k-250115"),
            smart_model=os.getenv("DOUBAO_SMART_MODEL", "doubao-1-5-pro-32k-250115"),
        )

    if name == "deepseek":
        raise LLMUnavailable(
            "DeepSeek 已停用（公共 API 不支持多模态视觉）。请使用 LLM_PROVIDER=qwen。"
        )

    raise LLMUnavailable(f"不支持的 LLM_PROVIDER: {name}")


def make_vision_provider(api_key: str | None = None) -> BaseProvider | None:
    """构建多模态 provider。优先用主 provider（如其支持视觉），否则看 VISION_PROVIDER 环境变量。
    都没有返回 None，调用方应降级处理（不阻断）。
    api_key 显式传入时覆盖环境变量（key pool 多窗口并发场景用）。"""
    try:
        main = make_provider(api_key=api_key)
    except LLMUnavailable:
        main = None
    if main is not None and main.supports_vision():
        return main
    name = os.getenv("VISION_PROVIDER", "").lower().strip()
    if not name:
        return None
    try:
        candidate = make_provider(name, api_key=api_key)
    except LLMUnavailable:
        return None
    return candidate if candidate.supports_vision() else None
