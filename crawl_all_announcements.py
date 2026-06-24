"""Full Cninfo announcement metadata crawler.

The full crawler fetches by day, stores metadata only, and never downloads PDFs.
A date is completed only after every page succeeds. If one page keeps failing
after retries, the date is marked failed and can be re-crawled next run.
"""

from __future__ import annotations

import argparse
import logging
import math
import random
import time
from typing import Any

from cninfo_models import normalize_raw_announcement
from cninfo_service import CninfoCrawlerService, CrawlConfig, summarize_record
from crawl_state import advance_backfill_if_success, load_state, mark_date_status, resolve_dates, save_state
from storage_csv import merge_raw_records

LOGGER = logging.getLogger(__name__)


def build_payload(day: str, page: int, config: CrawlConfig) -> dict[str, Any]:
    return {
        "pageNum": page,
        "pageSize": config.page_size,
        "column": config.column,
        "tabName": config.tab_name,
        "searchkey": "",
        "seDate": f"{day}~{day}",
        "sortName": config.sort_name,
        "sortType": config.sort_type,
        "isHLtitle": "true",
    }


def request_page_with_retry(
    service: CninfoCrawlerService,
    payload: dict[str, Any],
    config: CrawlConfig,
    day: str,
    page: int,
    max_retries: int,
    retry_backoffs: list[float],
) -> dict[str, Any]:
    attempt = 0
    while True:
        try:
            response = service.session.post(
                service.url,
                data=payload,
                headers=service.headers,
                proxies=config.proxies,
                timeout=config.request_timeout,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                raise RuntimeError(f"date={day} page={page} failed after {max_retries} retries: {exc}") from exc
            sleep_seconds = retry_backoffs[min(attempt - 1, len(retry_backoffs) - 1)]
            LOGGER.warning(
                "日期 %s 第 %s 页请求失败，第 %s/%s 次重试将在 %.1f 秒后执行：%s",
                day,
                page,
                attempt,
                max_retries,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


def fetch_all_records_for_day(
    service: CninfoCrawlerService,
    day: str,
    config: CrawlConfig,
    max_pages_per_day: int,
    max_retries: int,
    retry_backoffs: list[float],
) -> tuple[list[dict[str, str]], int]:
    page = 1
    all_records: list[dict[str, str]] = []
    seen_page_signatures: set[tuple[str, ...]] = set()

    while True:
        if page > max_pages_per_day:
            raise RuntimeError(f"date={day} exceeded max_pages_per_day={max_pages_per_day}")

        LOGGER.info("开始抓取全量公告第 %s 页，日期 %s，当前已抓取 %s 条。", page, day, len(all_records))
        payload = build_payload(day, page, config)
        response_json = request_page_with_retry(service, payload, config, day, page, max_retries, retry_backoffs)
        announcements = response_json.get("announcements", [])
        has_more = bool(response_json.get("hasMore", False))
        if not announcements:
            if has_more:
                raise RuntimeError(f"date={day} page={page} returned no announcements while hasMore=true")
            LOGGER.info("日期 %s 第 %s 页无数据，结束该日抓取。", day, page)
            break

        LOGGER.info("日期 %s 第 %s 页返回 %s 条原始记录。", day, page, len(announcements))
        page_records: list[dict[str, str]] = []
        for item in announcements:
            record = normalize_raw_announcement(item, column=config.column)
            if record.get("title") and record.get("publish_date"):
                page_records.append(record)

        if page_records:
            page_signature = tuple(record.get("announcement_id", "") for record in page_records)
            if page_signature in seen_page_signatures:
                raise RuntimeError(f"date={day} page={page} repeated page signature; stop this date for next recrawl")
            seen_page_signatures.add(page_signature)
            LOGGER.info(
                "日期 %s 第 %s 页第一条数据：%s",
                day,
                page,
                summarize_record(
                    {
                        "keyword": "",
                        "stock_code": page_records[0].get("stock_code", ""),
                        "stock_name": page_records[0].get("stock_name", ""),
                        "title": page_records[0].get("title", ""),
                        "publish_time": page_records[0].get("publish_date", ""),
                        "announcement_url": page_records[0].get("announcement_url", ""),
                    }
                ),
            )
            all_records.extend(page_records)
            LOGGER.info("日期 %s 抓取累计有效记录 %s 条。", day, len(all_records))
        else:
            LOGGER.warning("日期 %s 第 %s 页记录均被过滤，未产生有效数据。", day, page)

        if not has_more:
            LOGGER.info("日期 %s 已抓取完毕，共 %s 条有效记录。", day, len(all_records))
            break

        page += 1
        time.sleep(random.uniform(*config.delay_range))

    return all_records, page


def crawl_one_day(
    service: CninfoCrawlerService,
    day: str,
    column: str,
    page_size: int,
    delay_min: float,
    delay_max: float,
    max_pages_per_day: int,
    max_retries: int,
    retry_backoffs: list[float],
) -> dict[str, Any]:
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
    records, pages = fetch_all_records_for_day(
        service,
        day,
        config,
        max_pages_per_day=max_pages_per_day,
        max_retries=max_retries,
        retry_backoffs=retry_backoffs,
    )
    storage_stats = merge_raw_records(records)
    return {
        "status": "completed",
        "pages": pages,
        "pages_estimate": math.ceil(len(records) / page_size) if page_size else 0,
        "records": len(records),
        "storage": storage_stats,
    }


def parse_retry_backoffs(value: str) -> list[float]:
    backoffs = [float(item.strip()) for item in value.split(",") if item.strip()]
    return backoffs or [3.0, 6.0, 12.0]


def has_run_page_budget(pages_used: int, max_pages_per_run: int) -> bool:
    if max_pages_per_run <= 0:
        return True
    return pages_used < max_pages_per_run


def page_limit_for_next_date(max_pages_per_day: int, pages_used: int, max_pages_per_run: int) -> int:
    if max_pages_per_run <= 0:
        return max_pages_per_day
    remaining_pages = max_pages_per_run - pages_used
    return max(0, min(max_pages_per_day, remaining_pages))


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
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-backoffs", default="3,6,12")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    state = load_state()
    dates = resolve_dates(state, args.mode, args.hot_window_days, args.verify_window_days, args.batch_days)
    limits = state.setdefault("limits", {})
    max_days_per_run = int(limits.get("max_days_per_run", 7))
    max_pages_per_day = int(limits.get("max_pages_per_day", 3000))
    max_pages_per_run = int(limits.get("max_pages_per_run", 8000))
    if len(dates) > max_days_per_run:
        LOGGER.warning("日期数量 %s 超过 max_days_per_run=%s，将截断。", len(dates), max_days_per_run)
        dates = dates[-max_days_per_run:]

    retry_backoffs = parse_retry_backoffs(args.retry_backoffs)
    service = CninfoCrawlerService()
    failures = 0
    pages_used = 0
    for day in dates:
        if not has_run_page_budget(pages_used, max_pages_per_run):
            LOGGER.warning(
                "本次累计页数 %s 已达到 max_pages_per_run=%s，停止继续抓取后续日期。",
                pages_used,
                max_pages_per_run,
            )
            break
        effective_max_pages = page_limit_for_next_date(max_pages_per_day, pages_used, max_pages_per_run)
        try:
            result = crawl_one_day(
                service,
                day,
                args.column,
                args.page_size,
                args.delay_min,
                args.delay_max,
                max_pages_per_day=effective_max_pages,
                max_retries=args.max_retries,
                retry_backoffs=retry_backoffs,
            )
            mark_date_status(state, day, result["status"], result)
            pages_used += int(result.get("pages", 0))
        except Exception as exc:
            LOGGER.exception("日期 %s 抓取失败；不写入该日新数据，不推进回填水位，下次将重新抓取该日。", day)
            mark_date_status(state, day, "failed", {"error": str(exc)})
            failures += 1

    if args.mode == "backfill":
        advance_backfill_if_success(state, dates)
    else:
        completed_dates = [day for day in dates if state.get("date_status", {}).get(day, {}).get("status") == "completed"]
        state.setdefault("daily", {})["last_success_range"] = {
            "start": min(completed_dates) if completed_dates else None,
            "end": max(completed_dates) if completed_dates else None,
        }
    save_state(state)
    if failures:
        LOGGER.error("本次存在失败日期：%s", failures)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
