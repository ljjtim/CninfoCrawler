"""Local keyword filtering for raw Cninfo announcement metadata."""

from __future__ import annotations

import argparse
import logging
from typing import Iterable

import pandas as pd

from cninfo_models import FILTERED_COLUMNS, clean_text
from cninfo_service import DEFAULT_KEYWORDS
from storage_csv import load_all_raw, write_filtered_exports, write_legacy_announcements

LOGGER = logging.getLogger(__name__)
MATCH_FIELDS = ["title", "stock_code", "stock_name", "category"]


def match_keywords(row: pd.Series, keywords: Iterable[str]) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
    for keyword in keywords:
        keyword = clean_text(keyword)
        if not keyword:
            continue
        matched_fields = [field for field in MATCH_FIELDS if keyword in clean_text(row.get(field, ""))]
        if not matched_fields:
            continue
        hits.append(
            {
                "announcement_id": clean_text(row.get("announcement_id", "")),
                "keyword": keyword,
                "matched_field": "+".join(matched_fields),
                "publish_date": clean_text(row.get("publish_date", "")),
                "stock_code": clean_text(row.get("stock_code", "")),
                "stock_name": clean_text(row.get("stock_name", "")),
                "title": clean_text(row.get("title", "")),
                "announcement_url": clean_text(row.get("announcement_url", "")),
            }
        )
    return hits


def filter_raw_announcements(keywords: list[str]) -> pd.DataFrame:
    raw_df = load_all_raw()
    if raw_df.empty:
        return pd.DataFrame(columns=FILTERED_COLUMNS)
    records: list[dict[str, str]] = []
    for _, row in raw_df.iterrows():
        records.extend(match_keywords(row, keywords))
    filtered_df = pd.DataFrame(records, columns=FILTERED_COLUMNS)
    if filtered_df.empty:
        return filtered_df
    filtered_df = filtered_df.drop_duplicates(subset=["announcement_id", "keyword"], keep="last")
    return filtered_df


def main() -> int:
    parser = argparse.ArgumentParser(description="Filter raw Cninfo announcements by local keywords.")
    parser.add_argument("--keywords", nargs="*", default=list(DEFAULT_KEYWORDS), help="Keywords matched against title/code/name/category")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    filtered_df = filter_raw_announcements(args.keywords)
    write_filtered_exports(filtered_df)
    write_legacy_announcements(filtered_df)
    LOGGER.info("本地关键词过滤完成，命中 %s 条。", len(filtered_df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
