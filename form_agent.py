"""form_agent.py — pypdf AcroForm fill with a Python Formatter Registry.

Loads forms/{form_id}/field_map.json. Each entry in `candidate_mapping` is either:

  Simple mapping (1:1):
    "name_en": "英文姓名 Name In English"

  Sentinel:
    "email":   "NOT_IN_FORM"        — form has no such field, skipped entirely
    "X":       "FILL_AFTER_SCAN"    — known gap, skipped from totals

  Formatter spec (deterministic transformation → one or many PDF fields):
    "date_of_birth": {"formatter": "strip_dashes", "target": "出生日期 ..."}
    "ccc_code":      {"formatter": "split_by_space", "targets": ["...Code_1", "...Code_2", ...]}
    "address":       {"formatter": "hk_address_ra", "targets": {"building": "...", ...}}

Formatters are deterministic Python functions (NOT LLM calls). They take the
raw candidate value + target descriptor → return {pdf_field_name: value, ...}.
Add new formatters by decorating a function with @_formatter("name").

Returns (pdf_bytes, filled_count, total_count, missing_labels) for partial-fill reporting.
"""
import json
import logging
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Callable

from pypdf import PdfReader, PdfWriter
from pypdf.generic import BooleanObject, NameObject

from address_parser import split_hk_address

log = logging.getLogger(__name__)

FORMS_DIR = Path(os.environ.get("FORMS_DIR") or (Path(__file__).parent / "forms"))

_SKIP_IN_FORM = "NOT_IN_FORM"
_FILL_AFTER_SCAN = "FILL_AFTER_SCAN"

_CANDIDATE_FIELD_LABELS = {
    "name_en": "英文姓名 Name in English",
    "name_zh": "中文姓名 Name in Chinese",
    "hkid": "身份證號碼 HKID",
    "date_of_birth": "出生日期 Date of Birth",
    "phone": "聯絡電話 Phone",
    "email": "電郵 Email",
    "address": "地址 Address",
    "ccc_code": "中文電碼 CCC",
    "driving_licence": "駕駛執照號碼 Driving Licence No",
    "licence_classes": "牌照類別 Licence Classes",
    "licence_expiry": "牌照到期日 Licence Expiry",
}

# ─────────────────────────── Formatter Registry ───────────────────────────
# Each formatter: (value, target_descriptor) → {pdf_field_name: str_value, ...}
# target_descriptor shape depends on formatter (str / list / dict).
_FORMATTERS: dict[str, Callable] = {}


def _formatter(name: str):
    def _wrap(fn: Callable) -> Callable:
        _FORMATTERS[name] = fn
        return fn
    return _wrap


@_formatter("strip_dashes")
def _fmt_strip_dashes(val, target: str) -> dict:
    """1999-09-10 → 19990910. For comb-style date fields (AcroForm Comb flag)."""
    return {target: str(val).replace("-", "").replace("/", "").replace(" ", "")}


@_formatter("split_by_space")
def _fmt_split_by_space(val, targets: list) -> dict:
    """Split CCC / similar grouped codes into N target fields.
    Handles two OCR cases:
      '6079 4993 6900' (spaced)        → ['6079','4993','6900']
      '172814986134'   (no-space blob) → ['1728','1498','6134'] when len = N*4 digits
    """
    s = str(val).strip()
    parts = s.split()
    if len(parts) == 1 and parts[0].isdigit():
        digits = parts[0]
        n = len(targets)
        if n > 0 and len(digits) == n * 4:
            parts = [digits[i * 4:(i + 1) * 4] for i in range(n)]
    return {t: p for t, p in zip(targets, parts) if p}


@_formatter("date_to_comb")
def _fmt_date_to_comb(val, target: str) -> dict:
    """ISO '1999-09-10' → '10091999' for DDMMYYYY 8-cell comb (default HK gov layout).
    Accepts ISO (yyyy-mm-dd), DMY (dd-mm-yyyy), or any '-' / '/' / '.' / space separator.
    Day & month zero-padded; year kept as-is (typically 4-digit).
    """
    parts = re.split(r"[-/\s.]+", str(val).strip())
    if len(parts) < 3 or not target:
        return {}
    if len(parts[0]) == 4:                       # ISO yyyy-mm-dd
        yyyy, mm, dd = parts[0], parts[1], parts[2]
    else:                                        # already DMY
        dd, mm, yyyy = parts[0], parts[1], parts[2]
    return {target: f"{dd.zfill(2)}{mm.zfill(2)}{yyyy}"}


@_formatter("split_date_to_3")
def _fmt_split_date_to_3(val, targets: dict) -> dict:
    """ISO '1999-09-10' or DMY '10/09/1999' → 3 separate PDF fields.
    targets shape: {"dd": pdf_field, "mm": pdf_field, "yyyy": pdf_field}.
    Order auto-detected: 4-digit prefix → ISO; else DMY.
    """
    parts = re.split(r"[-/\s.]+", str(val).strip())
    if len(parts) < 3:
        return {}
    if len(parts[0]) == 4:
        yyyy, mm, dd = parts[0], parts[1], parts[2]
    else:
        dd, mm, yyyy = parts[0], parts[1], parts[2]
    bag = {"dd": dd, "mm": mm, "yyyy": yyyy}
    return {pdf_field: bag[k] for k, pdf_field in targets.items()
            if pdf_field and k in bag and bag[k]}


@_formatter("tickbox_group")
def _fmt_tickbox_group(val, targets: dict) -> dict:
    """Mutually-exclusive checkbox group (e.g. Mr/Mrs/Miss/Ms).
    targets shape: {value_a: pdf_field_a, value_b: pdf_field_b, ...}.
    Returns {pdf_field: '/Yes' for matching value, '/Off' for all OTHERS}.
    Empty dict if val not in targets — caller flags missing.
    Note: '/Yes' / '/Off' assume standard PDF AcroForm export values; if a form
    uses a custom ON name (rare), surface it during human review.
    """
    val_str = str(val).strip()
    if not val_str or val_str not in targets:
        return {}
    return {pdf_field: ("/Yes" if v_key == val_str else "/Off")
            for v_key, pdf_field in targets.items() if pdf_field}


@_formatter("tickbox_single")
def _fmt_tickbox_single(val, target: str) -> dict:
    """Single boolean checkbox. target = single PDF field name (string).
    Truthy → '/Yes', falsy / 'false' / 'no' / '0' → '/Off'.
    """
    if not target:
        return {}
    s = str(val).strip().lower()
    truthy = s not in ("", "false", "0", "no", "off", "none", "null")
    return {target: "/Yes" if truthy else "/Off"}


@_formatter("hk_address_ra")
def _fmt_hk_address_ra(val, targets: dict) -> dict:
    """HK address string → 6 RA sub-fields via LLM parser.
    targets shape: {"building": "pdf_field_name", "street": "...", ...}
    Fallback on parser failure: dump whole address into building target.
    """
    parts = split_hk_address(str(val))
    if parts:
        out = {pdf_field: str(parts[part_key])
               for part_key, pdf_field in targets.items()
               if parts.get(part_key)}
        if out:
            return out
    first_target = targets.get("building") or next(iter(targets.values()), None)
    return {first_target: str(val)} if first_target else {}


# ───────────────────────────────── Core ──────────────────────────────────
def _load_field_map(form_id: str) -> dict:
    path = FORMS_DIR / form_id / "field_map.json"
    if not path.exists():
        raise FileNotFoundError(f"field_map.json not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_field_values(candidate: dict, field_map: dict) -> tuple[dict, int, int, list[str]]:
    """Return (field_values, filled_count, total_count, missing_labels)."""
    mapping = field_map.get("candidate_mapping", {})
    field_values: dict[str, str] = {}
    filled_count = 0
    total_count = 0
    missing: list[str] = []

    # Conditional-skip context: non-Chinese candidates have no CCC to report.
    name_zh_present = bool(str(candidate.get("name_zh") or "").strip())

    for cand_key, spec in mapping.items():
        # ─ 1. Parse mapping spec ─
        if isinstance(spec, str):
            if spec in (_SKIP_IN_FORM, _FILL_AFTER_SCAN):
                continue
            formatter_fn = None
            target = spec
        elif isinstance(spec, dict):
            fmt_name = spec.get("formatter")
            formatter_fn = _FORMATTERS.get(fmt_name) if fmt_name else None
            if not formatter_fn:
                log.warning("Unknown formatter %r for %s — skipping", fmt_name, cand_key)
                continue
            target = spec.get("target") if "target" in spec else spec.get("targets")
            if target is None:
                log.warning("Formatter %s missing target/targets for %s", fmt_name, cand_key)
                continue
        else:
            continue

        # ─ 2. Conditional skip: non-Chinese candidate has no CCC ─
        if cand_key == "ccc_code" and not name_zh_present:
            continue

        total_count += 1
        val = candidate.get(cand_key)
        if val is None or str(val).strip() == "":
            missing.append(_CANDIDATE_FIELD_LABELS.get(cand_key, cand_key))
            continue

        # ─ 3. Apply formatter or plain 1:1 fill ─
        if formatter_fn:
            try:
                new_values = formatter_fn(val, target)
            except Exception as e:
                log.warning("Formatter failed for %s: %s", cand_key, e)
                missing.append(_CANDIDATE_FIELD_LABELS.get(cand_key, cand_key))
                continue
            if not new_values:
                missing.append(_CANDIDATE_FIELD_LABELS.get(cand_key, cand_key))
                continue
            field_values.update({k: str(v) for k, v in new_values.items() if v})
        else:
            field_values[target] = str(val)

        filled_count += 1

    return field_values, filled_count, total_count, missing


def _set_need_appearances(writer: PdfWriter) -> None:
    """Force PDF viewers to regenerate field appearance streams on open.
    Needed for reliable rendering across Adobe / Preview / Chrome / Drive."""
    catalog = writer._root_object
    if NameObject("/AcroForm") in catalog:
        catalog[NameObject("/AcroForm")][NameObject("/NeedAppearances")] = BooleanObject(True)


def fill_form(form_id: str, candidate: dict) -> tuple[bytes, int, int, list[str]]:
    """Fill a PDF AcroForm with candidate data.

    Returns (pdf_bytes, filled_count, total_count, missing_labels).
    """
    template_path = FORMS_DIR / form_id / "template.pdf"
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    field_map = _load_field_map(form_id)
    field_values, filled, total, missing = _build_field_values(candidate, field_map)

    reader = PdfReader(str(template_path))
    writer = PdfWriter()
    writer.append(reader)
    _set_need_appearances(writer)

    if field_values:
        for page in writer.pages:
            writer.update_page_form_field_values(page, field_values, auto_regenerate=True)
        log.info(
            "Form %s: filled %d/%d candidate fields (%d PDF values written)",
            form_id, filled, total, len(field_values),
        )
    else:
        log.warning("Form %s: no fields filled — check candidate_mapping", form_id)

    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue(), filled, total, missing
