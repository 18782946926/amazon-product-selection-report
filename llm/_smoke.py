"""手动冒烟测试：验证当前配置的 LLM Provider 能正常工作。

用法：
    cd web_report_generator
    python -m llm._smoke

需先在 .env 中配置 LLM_PROVIDER 和对应的 API_KEY。
"""
from __future__ import annotations

import logging
import sys

from llm import make_client
from llm.exceptions import LLMSchemaError, LLMUnavailable
from llm.schemas import SmokeResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("smoke")


def test_text(client) -> bool:
    log.info("=== Test 1: text 模式 ===")
    try:
        out = client.chat(
            messages=[
                {"role": "system", "content": "你是简洁的助手，只回答一个词。"},
                {"role": "user", "content": "返回单词 pong"},
            ],
            tier="fast",
            max_tokens=20,
        )
        log.info("✓ text 模式响应: %r", out.strip())
        return True
    except LLMUnavailable as e:
        log.error("✗ text 模式失败: %s", e)
        return False


def test_json(client) -> bool:
    log.info("=== Test 2: json 模式 + pydantic 校验 ===")
    try:
        obj = client.chat_json(
            messages=[
                {"role": "system", "content": "你只输出 JSON，字段为 ok(bool) 和 echo(string)。"},
                {"role": "user", "content": '请输出 {"ok": true, "echo": "hello"}'},
            ],
            schema=SmokeResponse,
            tier="fast",
            max_tokens=100,
        )
        log.info("✓ json 模式响应: ok=%s echo=%r", obj.ok, obj.echo)
        return True
    except (LLMUnavailable, LLMSchemaError) as e:
        log.error("✗ json 模式失败: %s", e)
        return False


def test_smart_tier(client) -> bool:
    log.info("=== Test 3: smart-tier 模式 ===")
    try:
        out = client.chat(
            messages=[{"role": "user", "content": "用一句话说明你是哪个模型。"}],
            tier="smart",
            max_tokens=100,
        )
        log.info("✓ smart-tier 响应: %s", out.strip()[:200])
        return True
    except LLMUnavailable as e:
        log.error("✗ smart-tier 失败: %s", e)
        return False


def test_vision(client) -> bool:
    log.info("=== Test 4: 多模态视觉模式 ===")
    if not client.supports_vision():
        log.warning("当前 provider 不支持视觉，跳过")
        return True
    try:
        # 用一张公开的简单图（Wikipedia 的 1px 透明 PNG 不行，要有内容；改用一个公开演示图）
        url = "https://help-static-aliyun-doc.aliyuncs.com/file-manage-files/zh-CN/20241022/emyrja/dog_and_girl.jpeg"
        out = client.chat_multimodal_json(
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "图里有什么主要主体？只输出 JSON：{\"ok\": true, \"echo\": \"<10 字内描述>\"}"},
                    {"type": "image_url", "image_url": {"url": url}},
                ],
            }],
            schema=SmokeResponse,
            max_tokens=200,
            timeout=60,
        )
        if out is None:
            log.error("✗ 视觉模式返回 None（可能 schema 不符 / 网络）")
            return False
        log.info("✓ 视觉模式响应: ok=%s echo=%r", out.ok, out.echo)
        return True
    except Exception as e:
        log.error("✗ 视觉模式异常: %s", e)
        return False


def main() -> int:
    try:
        client = make_client()
    except LLMUnavailable as e:
        log.error("Provider 初始化失败: %s", e)
        log.error("请检查 .env 中的 LLM_PROVIDER 和对应 API_KEY 配置")
        return 2

    log.info("使用 Provider: %s (fast=%s, smart=%s)",
             client.provider.name, client.provider.fast_model, client.provider.smart_model)

    results = [
        test_text(client),
        test_json(client),
        test_smart_tier(client),
        test_vision(client),
    ]
    passed = sum(results)
    log.info("=== 冒烟结果: %d/%d 通过 ===", passed, len(results))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
