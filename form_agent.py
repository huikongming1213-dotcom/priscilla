"""form_agent.py — pypdf AcroForm fill, with HK address auto-split.

Loads forms/{form_id}/field_map.json:
  - candidate_mapping: {candidate_key: pdf_field_name}
  - mapping_notes.address_fields_ra: [6 PDF field names for residential address parts]
    (building, street, flat, floor, block, district — in that order)

Behavior:
  - NOT_IN_FORM → skipped entirely (form has no such field, not a "missing data" issue)
  - FILL_AFTER_SCAN → only 'address' is supported (via address_fields_ra); others skipped
  - For candidate_key "address": call address_parser → 6 parts → fill RA sub-fields
  - For all others: simple 1-to-1 field fill
  - Returns (pdf_bytes, filled_count, total_count, missing_labels) for partial-fill reporting
"""
import json
import logging
import os
from io import BytesIO
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from pypdf.generic import BooleanObject, NameObject

from address_parser import split_hk_address

log = logging.getLogger(__name__)

FORMS_DIR = Path(os.environ.get("FORMS_DIR") or (Path(__file__).parent / "forms"))

_SKIP_IN_FORM = "NOT_IN_FORM"
_FILL_AFTER_SCAN = "FILL_AFTER_SCAN"

# Order of keys returned by address_parser.split_hk_address — must match
# forms/{form_id}/field_map.json mapping_notes.address_fields_ra order.
_ADDRESS_PART_KEYS = ["building", "street", "flat", "floor", "block", "district"]

# Human-readable labels for candidate keys (surfaced to Priscilla via X-Fill-Missing header)
_CANDIDATE_FIELD_LABELS = {
    "name_en": "英文姓名 Name in English",
    "name_zh": "中文姓名 Name in Chinese",
    "hkid": "身份證號碼 HKID",
    "date_of_birth": "出生日期 Date of Birth",
    "phone": "聯絡電話 Phone",
    "email": "電郵 Email",
    "address": "地址 Address",
    "driving_licence": "駕駛執照號碼 Driving Licence No",
    "licence_classes": "牌照類別 Licence Classes",
    "licence_expiry": "牌照到期日 Licence Expiry",
}


def _load_field_map(form_id: str) -> dict:
    path = FORMS_DIR / form_id / "field_map.json"
    if not path.exists():
        raise FileNotFoundError(f"field_map.json not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_field_values(candidate: dict, field_map: dict) -> tuple[dict, int, int, list[str]]:
    """Return (field_values, filled_count, total_count, missing_labels).

    total_count = candidate keys the form actually supports (excludes NOT_IN_FORM and
                  unsupported FILL_AFTER_SCAN entries).
    filled_count = of those, how many got real data from candidate.
    missing_labels = human-readable labels for the (total - filled) gap.
    """
    mapping = field_map.get("candidate_mapping", {})
    notes = field_map.get("mapping_notes", {})
    field_values: dict[str, str] = {}
    filled_count = 0
    total_count = 0
    missing: list[str] = []

    for candidate_key, pdf_field_name in mapping.items():
        if pdf_field_name == _SKIP_IN_FORM:
            continue

        if pdf_field_name == _FILL_AFTER_SCAN:
            # Only 'address' has special-case support (via address_fields_ra)
            if not (candidate_key == "address" and notes.get("address_fields_ra")):
                continue

        total_count += 1
        val = candidate.get(candidate_key)
        if val is None or str(val).strip() == "":
            missing.append(_CANDIDATE_FIELD_LABELS.get(candidate_key, candidate_key))
            continue

        if candidate_key == "address":
            ra_fields = notes.get("address_fields_ra", [])
            parts = split_hk_address(str(val))
            if parts and len(ra_fields) == len(_ADDRESS_PART_KEYS):
                for pdf_field, part_key in zip(ra_fields, _ADDRESS_PART_KEYS):
                    part_val = parts.get(part_key)
                    if part_val:
                        field_values[pdf_field] = str(part_val)
            elif ra_fields:
                # Fallback: dump whole address into the building field
                field_values[ra_fields[0]] = str(val)
            filled_count += 1
            continue

        field_values[pdf_field_name] = str(val)
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
