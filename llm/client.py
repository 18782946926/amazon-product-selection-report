from __future__ import annotations

import json
import logging
from typing import Any, Literal, Type, TypeVar

from pydantic import BaseModel, ValidationError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from llm.cache import LLMCache
from llm.exceptions import LLMSchemaError, LLMUnavailable
from llm.providers.base import BaseProvider, ModelTier

log = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class LLMClient:
    def __init__(self, provider: BaseProvider, cache: LLMCache | None = None,
                 *, max_retries: int = 1, default_timeout: float = 60.0):
        self.provider = provider
        self.cache = cache
        self.max_retries = max_retries
        self.default_timeout = default_timeout

    def chat(
        self,
        messages: list[dict],
        *,
        tier: ModelTier = "fast",
        response_format: Literal["text", "json"] = "text",
        temperature: float = 0.3,
        max_tokens: int = 4096,
        timeout: float | None = None,
    ) -> str:
        timeout = timeout or self.default_timeout

        @retry(
            stop=stop_after_attempt(self.max_retries + 1),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=3),
            retry=retry_if_exception_type(LLMUnavailable),
            reraise=True,
        )
        def _call() -> str:
            return self.provider.chat(
                messages=messages,
                tier=tier,
                response_format=response_format,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
            )

        return _call()

    def supports_vision(self) -> bool:
        return self.provider.supports_vision()

    def chat_multimodal_json(
        self,
        messages: list[dict],
        *,
        schema: Type[T],
        cache_key: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        timeout: float | None = None,
    ) -> T | None:
        """多模态 + JSON 输出 + 缓存。失败返回 None（不抛），上层退化。
        messages 的 content 必须是 OpenAI 格式数组：[{type:text,...},{type:image_url,...}]。
        """
        if not self.provider.supports_vision():
            return None
        timeout = timeout or self.default_timeout

        if cache_key and self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                clean = {k: v for k, v in cached.items() if k != "reviewed"}
                try:
                    return schema.model_validate(clean)
                except ValidationError:
                    log.warning("视觉缓存命中但 schema 校验失败，删除缓存重新生成: %s", cache_key)
                    self.cache.delete(cache_key)

        try:
            raw = self.provider.chat_multimodal(
                messages=messages,
                response_format="json",
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
            )
        except (LLMUnavailable, NotImplementedError) as e:
            log.warning("视觉调用失败，跳过：%s", e)
            return None

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            repaired = _repair_truncated_json(raw)
            if repaired is None:
                log.warning("视觉返回非合法 JSON，跳过；raw=%s", raw[:200])
                return None
            try:
                data = json.loads(repaired)
            except json.JSONDecodeError:
                log.warning("视觉返回 JSON 修复失败，跳过；raw=%s", raw[:200])
                return None

        try:
            obj = schema.model_validate(data)
        except ValidationError as e:
            log.warning("视觉返回 JSON 不符合 schema，跳过：%s", e)
            return None

        if cache_key and self.cache:
            self.cache.set(cache_key, obj.model_dump(mode="json"))

        return obj

    def chat_json(
        self,
        messages: list[dict],
        *,
        schema: Type[T],
        tier: ModelTier = "fast",
        cache_key: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        timeout: float | None = None,
    ) -> T:
        if cache_key and self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                clean = {k: v for k, v in cached.items() if k != "reviewed"}
                try:
                    return schema.model_validate(clean)
                except ValidationError:
                    log.warning("缓存命中但 schema 校验失败，删除缓存重新生成: %s", cache_key)
                    self.cache.delete(cache_key)

        raw = self.chat(
            messages=messages,
            tier=tier,
            response_format="json",
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            # 截断恢复：LLM 达到 max_tokens 会从中间截断导致 JSON 不完整。
            # 尝试 1) 按有效前缀解析 2) 自动补齐未闭合的 `{` `[`
            repaired = _repair_truncated_json(raw)
            if repaired is not None:
                try:
                    data = json.loads(repaired)
                    log.warning("LLM JSON 截断，已按有效前缀补齐 %d 字符", len(repaired))
                except json.JSONDecodeError:
                    raise LLMSchemaError(f"LLM 返回非合法 JSON: {e}; raw={raw[:200]}") from e
            else:
                raise LLMSchemaError(f"LLM 返回非合法 JSON: {e}; raw={raw[:200]}") from e

        try:
            obj = schema.model_validate(data)
        except ValidationError as e:
            raise LLMSchemaError(f"LLM 返回 JSON 不符合 schema: {e}") from e

        if cache_key and self.cache:
            self.cache.set(cache_key, obj.model_dump(mode="json"))

        return obj


def _repair_truncated_json(raw: str) -> str | None:
    """尝试修复 LLM 因 max_tokens 截断产生的残缺 JSON。
    扫描括号/字符串状态，在最后一个合法的对象边界处截断，并补齐未闭合的 `{` `[`。
    成功返回可 parse 的字符串，否则 None。
    """
    s = raw.lstrip()
    if not s or s[0] not in "{[":
        return None
    stack: list[str] = []
    in_str = False
    escape = False
    last_safe = -1  # 最后一个"顶层对象刚闭合"的位置
    for i, ch in enumerate(s):
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if not stack:
                break
            stack.pop()
            if not stack:
                last_safe = i
        elif ch == "," and len(stack) == 1:
            # 在顶层数组/对象里的分隔符后也是安全切点
            last_safe = i - 1
    # 1) 如果能找到完整顶层对象
    if last_safe >= 0 and not stack:
        return s[: last_safe + 1]
    # 2) 在字符串中间截断：先把字符串封口
    trimmed = s
    if in_str:
        # 回退到最近的 `"`，连同其后内容一并丢弃
        q = trimmed.rfind('"')
        if q > 0:
            trimmed = trimmed[:q]
        in_str = False
    # 清除末尾半截的键/值/逗号，直到遇到 `,` `[` `{` 的前一字符
    while trimmed and trimmed[-1] not in "[{,":
        trimmed = trimmed[:-1]
    # 去除孤零零的尾逗号
    while trimmed and trimmed[-1] == ",":
        trimmed = trimmed[:-1]
    # 重新扫一遍 stack，闭合
    stack2: list[str] = []
    in_str2 = False
    escape2 = False
    for ch in trimmed:
        if in_str2:
            if escape2:
                escape2 = False
            elif ch == "\\":
                escape2 = True
            elif ch == '"':
                in_str2 = False
            continue
        if ch == '"':
            in_str2 = True
        elif ch in "{[":
            stack2.append(ch)
        elif ch in "}]":
            if stack2:
                stack2.pop()
    if in_str2 or not stack2:
        return trimmed if not stack2 else None
    closers = {"{": "}", "[": "]"}
    tail = "".join(closers[c] for c in reversed(stack2))
    return trimmed + tail


def make_client(api_key: str | None = None) -> LLMClient:
    """按环境变量构建 LLMClient（含 Provider + Cache + 重试配置）。
    api_key 显式传入时覆盖环境变量，让 key pool 给每个请求分配独立 key。"""
    from config.llm_config import load_settings, make_provider
    settings = load_settings()
    provider = make_provider(settings.provider, api_key=api_key)
    cache = LLMCache(settings.cache_dir)
    return LLMClient(
        provider=provider,
        cache=cache,
        max_retries=settings.max_retries,
        default_timeout=settings.timeout_seconds,
    )
