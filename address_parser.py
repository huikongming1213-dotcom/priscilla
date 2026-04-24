"""address_parser.py — Split HK address into 6 structured parts via OpenRouter LLM.

Returns: dict with keys building / street / flat / floor / block / district.
         Returns None on any failure (caller falls back to dumping whole string).

Missing OPEN_ROUTER_API_KEY is treated as a soft failure — logs a warning,
returns None so the caller can still produce a PDF.
"""
import json
import logging
import os
import re

import httpx

log = logging.getLogger(__name__)

_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_MODEL = os.environ.get("ADDRESS_PARSER_MODEL", "anthropic/claude-sonnet-4-5")
_TIMEOUT = 15.0

_PROMPT_TEMPLATE = """你係香港地址拆解器。將以下地址拆成 6 part 純 JSON，唔要任何解釋、唔要 markdown：

{{
  "building": "大廈/屋苑名稱",
  "street": "門牌號數及街道名（例：大埔道78號）",
  "flat": "室",
  "floor": "樓（例：12/F 填 12）",
  "block": "座/Tower（例：A座 填 A）",
  "district": "地區（例：沙田/觀塘/中環）"
}}

搵唔到嘅欄位填 ""（空字串）。

地址：
{address}"""

_KEYS = ["building", "street", "flat", "floor", "block", "district"]


def split_hk_address(address: str) -> dict | None:
    api_key = os.environ.get("OPEN_ROUTER_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        log.warning("OPEN_ROUTER_API_KEY not set — skipping address parse")
        return None

    if not address or not address.strip():
        return None

    try:
        resp = httpx.post(
            _OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": _MODEL,
                "max_tokens": 256,
                "messages": [
                    {"role": "user", "content": _PROMPT_TEMPLATE.format(address=address)}
                ],
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        match = re.search(r"\{[\s\S]*\}", content)
        if not match:
            log.warning("Address parser returned non-JSON: %s", content[:200])
            return None
        parts = json.loads(match.group(0))
        return {k: (parts.get(k) or "") for k in _KEYS}
    except Exception as e:
        log.exception("Address parse failed: %s", e)
        return None
