"""llm_field_mapper.py — Design-time LLM agent that proposes a field_map.json.

Run ONCE per form (or per form-template revision), NOT at runtime.
Runtime fill_form() stays 100% deterministic — see form_agent.py.

Pipeline (called by scripts/scan_form.py --generate-mapping):

    fields metadata + page PNG (base64) + candidate schema
                       │
                       ▼
        anthropic/claude-haiku-4-5 via OpenRouter
                       │
                       ▼
       draft mapping with per-field confidence + tier

Tier classification:
    Tier 1 (auto_confirm)  : confidence ≥ 0.95 AND simple 1:1 text mapping
    Tier 2 (auto_validate) : confidence ≥ 0.85 AND uses a formatter
    Tier 3 (needs_review)  : confidence < 0.85 OR tickbox_group OR formatter unknown
"""
import base64
import json
import logging
import os
import re
from typing import Any

import httpx

log = logging.getLogger(__name__)

_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_MODEL = os.environ.get("LLM_MAPPER_MODEL", "anthropic/claude-haiku-4.5")
_TIMEOUT = 90.0  # vision multi-page can be slow

# Schema fed to the LLM so it knows what candidate data exists at runtime.
CANDIDATE_SCHEMA: dict[str, str] = {
    "name_en":         "string e.g. 'CHAN TAI MAN'",
    "name_zh":         "Chinese name string e.g. '陳大文' (may be empty for non-Chinese)",
    "hkid":            "HKID e.g. 'A123456(7)'",
    "date_of_birth":   "ISO date string e.g. '1999-09-10'",
    "phone":           "string e.g. '98765432'",
    "email":           "string",
    "address":         "HK address string e.g. 'Flat A, 12/F, Sun Hing Bldg, 78 Tai Po Road, Sham Shui Po'",
    "ccc_code":        "string e.g. '6079 4993 6900' (Chinese Commercial Code, 4 digits per Chinese char, space-separated)",
    "gender":          "'M' | 'F' (may be empty)",
    "title":           "'Mr' | 'Mrs' | 'Miss' | 'Ms' (may be empty)",
    "driving_licence": "string",
    "licence_classes": "comma-separated string e.g. '1,2,3'",
    "licence_expiry":  "ISO date string",
}

# Available deterministic formatters in form_agent.py — LLM picks among these.
AVAILABLE_FORMATTERS: dict[str, str] = {
    "strip_dashes":     "Remove '-', '/', spaces from value. Use when target is a single comb field (one PDF field that visually splits into N cells, e.g. 8-cell DOB).  spec: {formatter, target}",
    "split_by_space":   "Split value by spaces, fill each chunk into a corresponding target field. Use for CCC '6079 4993 6900' or any space-separated multi-field value.  spec: {formatter, targets: [list]}",
    "split_date_to_3":  "Split ISO date '1999-09-10' into {dd, mm, yyyy} and fill 3 separate fields. Use when DOB is rendered as 3 individual text fields rather than one comb.  spec: {formatter, targets: {dd, mm, yyyy}}",
    "hk_address_ra":    "Parse HK address into 6 sub-parts (building, street, flat, floor, block, district) via LLM. Use for residential address blocks.  spec: {formatter, targets: {building, street, flat, floor, block, district}}",
    "tickbox_group":    "Mutually-exclusive checkbox group (e.g. Mr/Mrs/Miss/Ms). Maps candidate value to the correct true box; ALL OTHER boxes in the group MUST be set to false (never null).  spec: {formatter, targets: {value_a: pdf_field_a, value_b: pdf_field_b, ...}, default_others_false: true}",
    "tickbox_single":   "Single boolean checkbox (e.g. 'I agree'). Spec: {formatter, target, true_when: 'expression describing when to tick'}",
    "_simple":          "Plain 1:1 string mapping — use a string value (the PDF field name) directly, no formatter wrapper. Best for name/HKID/phone/email when target is a single text field.",
    "NOT_IN_FORM":      "Sentinel string — use when this candidate attribute has NO corresponding field in the PDF (skip silently).",
    "FILL_AFTER_SCAN":  "Sentinel string — use when there IS a likely field but you can't determine which (defer to human review).",
}

_PROMPT = """你係 PDF 表格 mapping agent。我會俾你：
1. 一張 PDF 表格嘅所有 AcroForm fields（名 + type + 額外 metadata）
2. PDF 每頁嘅圖片（用嚟視覺對應 field name 同實際位置）
3. Runtime candidate 數據 schema（即係將來 fill 嘅時候會有咩資料）
4. 可用嘅 deterministic formatters 清單

你嘅任務：output 純 JSON，描述每個 candidate attribute 對應邊個 PDF field（或 fields），以及用邊個 formatter（如有需要）。

輸出 schema：
{{
  "candidate_mapping": {{
    "<candidate_key>": <spec>,
    ...
  }},
  "confidence": {{
    "<candidate_key>": <0.0 to 1.0>,
    ...
  }},
  "reasoning": {{
    "<candidate_key>": "一句中文解釋點解咁 map",
    ...
  }},
  "uncertain_fields": ["<pdf_field_name>", ...]
}}

<spec> 可以係：
- 純 string（PDF field name）= simple 1:1 mapping
- "NOT_IN_FORM" = 表格冇對應 field
- "FILL_AFTER_SCAN" = 唔肯定，留俾人手 review
- {{"formatter": "<name>", "target": "<pdf_field>"}} = 單 target formatter
- {{"formatter": "<name>", "targets": [...]或{{...}}}} = 多 target formatter

可用 formatters：
{formatters_doc}

Candidate schema（runtime 會有嘅資料）：
{candidate_doc}

PDF AcroForm fields（名 + 類型 + extra）：
{fields_doc}

判斷規則：
1. 政府 / 香港表格通常用中英對照欄位名（例如 "英文姓名 Name In English"）— 中英對應好強信號
2. 出生日期：如果係單 1 個 text field，多數係 comb（用 strip_dashes）；如果見到 3 個獨立 day/month/year field，用 split_date_to_3
3. CCC（中文商用號碼 / Chinese Commercial Code）：通常 _1 _2 _3 ... 排列，每個係 4-cell comb，用 split_by_space
4. Tickbox group：mutual-exclusive 嘅 button group（e.g. Mr/Mrs/Miss/Ms、男/女）→ tickbox_group + default_others_false: true
5. 唔肯定就用 "FILL_AFTER_SCAN"，唔好估
6. confidence 0.95+ = 極肯定（中英對照名 + simple 1:1）；0.85-0.94 = 用 formatter 但邏輯清楚；< 0.85 = 有歧義
7. 任何你冇 map 但你覺得可能有用嘅 PDF field，加入 uncertain_fields list

只 output 純 JSON object，唔好任何 markdown / 解釋 / preamble。"""


def _build_fields_doc(fields: dict[str, dict]) -> str:
    """Render the AcroForm fields dict as a compact bullet list for the prompt."""
    lines = []
    for name, meta in fields.items():
        ftype = meta.get("type", "?")
        cv = meta.get("current_value", "")
        extra = []
        if meta.get("comb_count"):
            extra.append(f"comb={meta['comb_count']}")
        if meta.get("page"):
            extra.append(f"p{meta['page']}")
        if cv:
            extra.append(f"cv={cv!r}")
        suffix = f"  ({', '.join(extra)})" if extra else ""
        lines.append(f"- [{ftype}] {name!r}{suffix}")
    return "\n".join(lines)


def _build_formatters_doc() -> str:
    return "\n".join(f"- {name}: {desc}" for name, desc in AVAILABLE_FORMATTERS.items())


def _build_candidate_doc() -> str:
    return "\n".join(f"- {k}: {v}" for k, v in CANDIDATE_SCHEMA.items())


def _build_messages(fields: dict, page_images_b64: list[bytes | str]) -> list[dict]:
    """Multimodal content: text prompt + each page image."""
    text_block = _PROMPT.format(
        formatters_doc=_build_formatters_doc(),
        candidate_doc=_build_candidate_doc(),
        fields_doc=_build_fields_doc(fields),
    )
    content: list[dict] = [{"type": "text", "text": text_block}]
    for img in page_images_b64:
        if isinstance(img, bytes):
            img = base64.b64encode(img).decode("ascii")
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{img}"},
        })
    return [{"role": "user", "content": content}]


def _classify_tier(spec: Any, confidence: float) -> str:
    """Tier 1 = auto_confirm; Tier 2 = auto_validate; Tier 3 = needs_review."""
    if confidence < 0.85:
        return "needs_review"
    if isinstance(spec, str):
        if spec in ("NOT_IN_FORM", "FILL_AFTER_SCAN"):
            return "needs_review" if spec == "FILL_AFTER_SCAN" else "auto_confirm"
        return "auto_confirm" if confidence >= 0.95 else "auto_validate"
    if isinstance(spec, dict):
        formatter = spec.get("formatter")
        if formatter in ("tickbox_group", "tickbox_single"):
            return "needs_review"
        if formatter in AVAILABLE_FORMATTERS:
            return "auto_validate"
        return "needs_review"
    return "needs_review"


def _parse_llm_json(content: str) -> dict:
    """Extract the first {...} JSON object from LLM output and parse it."""
    match = re.search(r"\{[\s\S]*\}", content)
    if not match:
        raise ValueError(f"LLM returned non-JSON content: {content[:300]}")
    return json.loads(match.group(0))


def generate_field_map(
    form_id: str,
    fields: dict[str, dict],
    page_images_b64: list[bytes | str],
) -> dict:
    """Call the LLM and return a draft field_map dict ready to dump as draft.json.

    Output dict shape:
        {
          "form_id": "td63a",
          "candidate_mapping": {...},                  # per CANDIDATE_SCHEMA keys
          "_llm_metadata": {
            "model": "...",
            "confidence": {key: float, ...},
            "reasoning": {key: str, ...},
            "tier": {key: "auto_confirm"|"auto_validate"|"needs_review"},
            "uncertain_fields": [...],
          },
          "fields": fields,                            # passthrough for downstream
        }

    Raises on missing API key, network failure, or unparseable LLM output —
    caller (scan_form.py) decides how to surface the error.
    """
    api_key = os.environ.get("OPEN_ROUTER_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPEN_ROUTER_API_KEY not set in environment")
    if not page_images_b64:
        raise ValueError("page_images_b64 is empty — vision input required")

    messages = _build_messages(fields, page_images_b64)
    log.info("Calling %s for form %s (%d fields, %d page images)",
             _MODEL, form_id, len(fields), len(page_images_b64))

    resp = httpx.post(
        _OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": _MODEL,
            "max_tokens": 4096,
            "messages": messages,
        },
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    parsed = _parse_llm_json(content)

    candidate_mapping = parsed.get("candidate_mapping") or {}
    confidence = parsed.get("confidence") or {}
    reasoning = parsed.get("reasoning") or {}
    uncertain = parsed.get("uncertain_fields") or []

    tier = {
        key: _classify_tier(spec, float(confidence.get(key, 0.0)))
        for key, spec in candidate_mapping.items()
    }

    log.info(
        "Tier breakdown: confirm=%d validate=%d review=%d",
        sum(1 for t in tier.values() if t == "auto_confirm"),
        sum(1 for t in tier.values() if t == "auto_validate"),
        sum(1 for t in tier.values() if t == "needs_review"),
    )

    return {
        "form_id": form_id,
        "candidate_mapping": candidate_mapping,
        "_llm_metadata": {
            "model": _MODEL,
            "confidence": confidence,
            "reasoning": reasoning,
            "tier": tier,
            "uncertain_fields": uncertain,
        },
        "fields": fields,
    }
