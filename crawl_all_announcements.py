"""Full Cninfo announcement metadata crawler.

First implementation is deliberately conservative: it reuses the existing
CninfoCrawlerService request path with searchkey="" and stores announcement
metadata only. It never downloads PDFs.
"""

from __future__ import annotations

import argparse
import logging
import math
from typing import Any

from cninfo_models import extract_announcement_id, normalize_announcement_url, sha1_text
from cninfo_service import CninfoCrawlerService, CrawlConfig
from crawl_state import advance_backfill_if_success, load_state, mark_date_status, resolve_dates, save_state
from storage_csv import merge_raw_records

LOGGER = logging.getLogger(__name__)


def legacy_record_to_raw(record: dict[str, str], column: str) -> dict[str, str]:
    url = normalize_announcement_url(record.get("announcement_url"))
    announcement_id = extract_announcement_id(url)
    stock_code = record.get("stock_code", "")
    title = record.get("title", "")
    publish_date = record.get("publish_time", "")
    if not announcement_id:
        announcement_id = sha1_text("|".join([stock_code, publish_date, title, url]))
    return {
        "announcement_id": announcement_id,
        "stock_code": stock_code,
        "stock_name": record.get("stock_name", ""),
        "title": title,
        "publish_date": publish_date,
        "publish_time_ms": "",
        "announcement_url": url,
        "adjunct_url": record.get("announcement_url", ""),
        "column": column,
        "category": "",
        "org_id": "",
        "raw_hash": sha1_text(str(sorted(record.items()))),
        "crawled_at": "",
    }


def crawl_one_day(service: CninfoCrawlerService, day: str, column: str, page_size: int, delay_min: float, delay_max: float) -> dict[str, Any]:
    config = CrawlConfig(
        start_date=day,
        end_date=day,
        keywords=[""],
        output_file="announcements.csv",
        force_update=True,
        delay_range=(delay_min, delay_max),
        page_size=page_size,
        column=column,
        request_timeout=15,
    )
    records = service._fetch_keyword_records_for_range("", day, day, config)
    raw_records = [legacy_record_to_raw(record, column=column) for record in records]
    storage_stats = merge_raw_records(raw_records)
    return {
        "status": "completed",
        "warning": "safe adapter uses existing cninfo_service with searchkey=''",
        "pages_estimate": math.ceil(len(records) / page_size) if page_size else 0,
        "records": len(records),
        "storage": storage_stats,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Crawl all Cninfo announcement metadata by day.")
    parser.add_argument("--mode", choices=["daily", "verify", "backfill"], required=True)
    parser.add_argument("--hot-window-days", type=int, default=3)
    parser.add_argument("--verify-window-days", type=int, default=14)
    parser.add_argument("--batch-days", type=int, default=7)
    parser.add_argument("--column", default="szse")
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--delay-min", type=float, default=1.5)
    parser.add_argument("--delay-max", type=float, default=4.0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    state = load_state()
    dates = resolve_dates(state, args.mode, args.hot_window_days, args.verify_window_days, args.batch_days)
    limits = state.setdefault("limits", {})
    max_days_per_run = int(limits.get("max_days_per_run", 7))
    if len(dates) > max_days_per_run:
        LOGGER.warning("日期数量 %s 超过 max_days_per_run=%s，将截断。", len(dates), max_days_per_run)
        dates = dates[-max_days_per_run:]

    service = CninfoCrawlerService()
    failures = 0
    for day in dates:
        try:
            result = crawl_one_day(service, day, args.column, args.page_size, args.delay_min, args.delay_max)
            mark_date_status(state, day, result["status"], result)
        except Exception as exc:
            LOGGER.exception("日期 %s 抓取失败", day)
            mark_date_status(state, day, "failed", {"error": str(exc)})
            failures += 1

    if args.mode == "backfill":
        advance_backfill_if_success(state, dates)
    else:
        state.setdefault("daily", {})["last_success_range"] = {"start": min(dates) if dates else None, "end": max(dates) if dates else None}
    save_state(state)
    if failures:
        LOGGER.error("本次存在失败日期：%s", failures)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
