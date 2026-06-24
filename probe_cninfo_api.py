"""Safe Cninfo API probe using the existing service wrapper.

This probe does not write data files and does not download PDFs.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from cninfo_service import CninfoCrawlerService, CrawlConfig
from cninfo_models import extract_announcement_id, normalize_announcement_url


def run_probe(day: str, max_pages: int, column: str) -> int:
    datetime.strptime(day, "%Y-%m-%d")
    config = CrawlConfig(
        start_date=day,
        end_date=day,
        keywords=[""],
        output_file="announcements.csv",
        force_update=True,
        delay_range=(0.1, 0.2),
        page_size=30,
        column=column,
        request_timeout=15,
    )
    service = CninfoCrawlerService()
    records = service._fetch_keyword_records_for_range("", day, day, config)[: max(1, max_pages) * 30]
    print("=" * 88)
    print("probe=empty_searchkey")
    print(f"date={day}")
    print(f"column={column}")
    print(f"max_pages={max_pages}")
    print(f"record_count={len(records)}")
    if records:
        sample = records[0]
        url = normalize_announcement_url(sample.get("announcement_url"))
        print(f"sample_keys={sorted(sample.keys())}")
        print(f"title={sample.get('title')}")
        print(f"publish_time={sample.get('publish_time')}")
        print(f"stock_code={sample.get('stock_code')} stock_name={sample.get('stock_name')}")
        print(f"announcement_url={url}")
        print(f"announcement_id={extract_announcement_id(url) or '<missing>'}")
    print("note=This safe probe validates empty searchkey through existing cninfo_service. Use logs for hasMore and pagination behavior.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe Cninfo API for full announcement crawling.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--column", default="szse")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return run_probe(args.date, args.max_pages, args.column)


if __name__ == "__main__":
    raise SystemExit(main())
