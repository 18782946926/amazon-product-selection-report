"""LLM API Key 池，支持多窗口并发跑报告时各自分一个独立 key。

设计：
- 启动时从 `DASHSCOPE_API_KEYS`（逗号分隔）读 N 个 key；空则 fallback 单 key `DASHSCOPE_API_KEY`
- 每个 `/upload` 请求 round-robin 拿一个 key（非阻塞）
- 每个 key 独立 8 路 semaphore，单 key 内的视觉 LLM 并发仍受钳，防止单 key 配额被一个请求打满
- N 请求 ≤ N keys 时，每个请求独占一 key 互不抢配额；N 请求 > N keys 时多出的请求共用 key、共享 semaphore
"""
from __future__ import annotations

import os
import threading
import logging

log = logging.getLogger(__name__)


class LLMKeyPool:
    def __init__(self, keys: list[str]):
        self.keys: list[str] = [k.strip() for k in keys if k and k.strip()]
        self._key_sems: dict[str, threading.Semaphore] = {
            k: threading.Semaphore(8) for k in self.keys
        }
        self._lock = threading.Lock()
        self._next_idx = 0

    def acquire(self) -> str | None:
        """Round-robin 取下一个 key。pool 空则返 None（调用方会 fallback 到环境变量）。"""
        if not self.keys:
            return None
        with self._lock:
            key = self.keys[self._next_idx % len(self.keys)]
            self._next_idx += 1
        return key

    def get_sem(self, key: str | None) -> threading.Semaphore | None:
        if not key:
            return None
        return self._key_sems.get(key)

    def size(self) -> int:
        return len(self.keys)


_POOL: LLMKeyPool | None = None
_POOL_LOCK = threading.Lock()


def get_pool() -> LLMKeyPool:
    """惰性单例。首次调用时读环境变量，之后复用。"""
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            return _POOL
        raw = (os.getenv("DASHSCOPE_API_KEYS") or "").strip()
        if raw:
            keys = [k.strip() for k in raw.split(",") if k.strip()]
        else:
            single = (os.getenv("DASHSCOPE_API_KEY") or "").strip()
            keys = [single] if single else []
        _POOL = LLMKeyPool(keys)
        if keys:
            log.warning("[key_pool] 已加载 %d 个 API key（round-robin 分配）", len(keys))
        else:
            log.warning("[key_pool] 未配置任何 API key，所有请求将走 LLM 降级")
        return _POOL
