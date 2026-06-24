"""Cninfo announcement normalization helpers for full metadata crawling."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

STATIC_CNINFO_BASE = "http://static.cninfo.com.cn/"
CNINFO_TIMEZONE = timezone(timedelta(hours=8))
RAW_COLUMNS = [
    "announcement_id",
    "stock_code",
    "stock_name",
    "title",
    "publish_date",
    "publish_time_ms",
    "announcement_url",
    "adjunct_url",
    "column",
    "category",
    "org_id",
    "raw_hash",
    "crawled_at",
]
FILTERED_COLUMNS = [
    "announcement_id",
    "keyword",
    "matched_field",
    "publish_date",
    "stock_code",
    "stock_name",
    "title",
    "announcement_url",
]
LEGACY_COLUMNS = [
    "keyword",
    "stock_code",
    "stock_name",
    "title",
    "publish_time",
    "announcement_url",
]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("<em>", "").replace("</em>", "").strip()


def normalize_stock_code(value: Any) -> str:
    """Normalize plain numeric Cninfo security codes to six digits.

    Excel may display or save codes such as 002868 as 2868. Cninfo stock,
    ETF, bond, and B-share security codes are generally six numeric digits,
    so pure 1-5 digit values are left-padded. Non-plain values are preserved.
    """
    cleaned = clean_text(value)
    if re.fullmatch(r"\d{1,5}", cleaned):
        return cleaned.zfill(6)
    return cleaned


def sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def normalize_announcement_url(url: Any) -> str:
    cleaned = clean_text(url).replace("\\", "/")
    if not cleaned:
        return ""
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"{STATIC_CNINFO_BASE}{cleaned.lstrip('/')}"
    parsed = urlsplit(cleaned)
    scheme = "http"
    path = re.sub(r"\.pdf$", ".PDF", parsed.path, flags=re.IGNORECASE)
    return urlunsplit((scheme, parsed.netloc, path, "", ""))


def extract_announcement_id(*values: Any) -> str:
    for value in values:
        cleaned = clean_text(value)
        if not cleaned:
            continue
        direct_match = re.fullmatch(r"\d{6,}", cleaned)
        if direct_match:
            return direct_match.group(0)
        url_match = re.search(r"/(\d{6,})\.pdf$", cleaned, flags=re.IGNORECASE)
        if url_match:
            return url_match.group(1)
    return ""


def parse_publish_time_ms(value: Any) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    try:
        return str(int(float(cleaned)))
    except (TypeError, ValueError):
        return ""


def publish_date_from_ms(value: Any) -> str:
    ms = parse_publish_time_ms(value)
    if not ms:
        return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=CNINFO_TIMEZONE).strftime("%Y-%m-%d")
    except (OSError, ValueError):
        return ""


def raw_hash_for_item(item: Mapping[str, Any]) -> str:
    payload = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
    return sha1_text(payload)


def normalize_raw_announcement(item: Mapping[str, Any], column: str = "") -> dict[str, str]:
    adjunct_url = clean_text(item.get("adjunctUrl"))
    announcement_url = normalize_announcement_url(adjunct_url)
    announcement_id = extract_announcement_id(
        item.get("announcementId"),
        item.get("announcement_id"),
        adjunct_url,
        announcement_url,
    )
    publish_time_ms = parse_publish_time_ms(item.get("announcementTime"))
    publish_date = publish_date_from_ms(publish_time_ms)
    title = clean_text(item.get("announcementTitle"))
    stock_code = normalize_stock_code(item.get("secCode"))
    stock_name = clean_text(item.get("secName"))
    category = clean_text(item.get("announcementTypeName") or item.get("category") or item.get("announcementType"))
    org_id = clean_text(item.get("orgId") or item.get("org_id"))

    if not announcement_id and announcement_url:
        announcement_id = sha1_text(announcement_url)
    if not announcement_id:
        announcement_id = sha1_text("|".join([stock_code, publish_date, title]))

    return {
        "announcement_id": announcement_id,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "title": title,
        "publish_date": publish_date,
        "publish_time_ms": publish_time_ms,
        "announcement_url": announcement_url,
        "adjunct_url": adjunct_url,
        "column": clean_text(column),
        "category": category,
        "org_id": org_id,
        "raw_hash": raw_hash_for_item(item),
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }


def page_signature(records: list[Mapping[str, Any]]) -> tuple[str, ...]:
    signature: list[str] = []
    for item in records:
        normalized = normalize_raw_announcement(item)
        signature.append(normalized.get("announcement_id", ""))
    return tuple(signature)
