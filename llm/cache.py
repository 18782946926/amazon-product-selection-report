from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


class LLMCache:
    """文件系统 KV 缓存，按内容哈希做 key。"""

    def __init__(self, cache_dir: str | Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def make_key(*parts: str | bytes) -> str:
        """把多段内容拼接后哈希，作为缓存 key。"""
        h = hashlib.sha256()
        for p in parts:
            if isinstance(p, str):
                h.update(p.encode("utf-8"))
            else:
                h.update(p)
            h.update(b"\x1f")
        return h.hexdigest()[:24]

    def _path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json"

    def get(self, key: str) -> dict[str, Any] | None:
        path = self._path(key)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def set(self, key: str, value: dict[str, Any], *, reviewed: bool = False) -> None:
        payload = {"reviewed": reviewed, **value}
        path = self._path(key)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    def delete(self, key: str) -> None:
        path = self._path(key)
        if path.exists():
            path.unlink()

    def purge_except(self, *keep_prefixes: str) -> int:
        """删除 cache_dir 下不以 keep_prefixes 任一项打头的 .json 文件。返回删除文件数。
        用于「用户重传 = 重跑」语义：清非 vision 缓存，但保留 vision_classify_*（同图分类不变 + 冷启动昂贵）。
        """
        deleted = 0
        for f in self.cache_dir.glob("*.json"):
            if any(f.name.startswith(p) for p in keep_prefixes):
                continue
            try:
                f.unlink()
                deleted += 1
            except OSError:
                pass
        return deleted

    @staticmethod
    def file_hash(file_path: str | Path) -> str:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()[:16]
