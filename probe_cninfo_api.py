"""巨潮接口安全探测脚本。

脚本只请求指定日期的前几页，不写数据文件，也不下载 PDF。
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from typing import Any

from cninfo_service import CninfoCrawlerService, CrawlConfig
from cninfo_models import extract_announcement_id, normalize_announcement_url


def build_probe_payload(day: str, page: int, column: str, include_searchkey: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pageNum": page,
        "pageSize": 30,
        "column": column,
        "tabName": "fulltext",
        "seDate": f"{day}~{day}",
        "sortName": "pubdate",
        "sortType": "desc",
        "isHLtitle": "true",
    }
    if include_searchkey:
        payload["searchkey"] = ""
    return payload


def probe_variant(
    service: CninfoCrawlerService,
    day: str,
    max_pages: int,
    column: str,
    include_searchkey: bool,
) -> dict[str, Any]:
    datetime.strptime(day, "%Y-%m-%d")
    config = CrawlConfig(start_date=day, end_date=day, column=column, request_timeout=15)
    pages: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    stopped_by_page_limit = False

    for page in range(1, max(1, max_pages) + 1):
        payload = build_probe_payload(day, page, column, include_searchkey)
        response = service.session.post(
            service.url,
            data=payload,
            headers=service.headers,
            proxies=config.proxies,
            timeout=config.request_timeout,
        )
        response.raise_for_status()
        response_json = response.json()
        announcements = response_json.get("announcements", []) or []
        has_more = bool(response_json.get("hasMore", False))
        pages.append(
            {
                "page": page,
                "payload": payload,
                "http_status": response.status_code,
                "hasMore": has_more,
                "record_count": len(announcements),
                "response_keys": sorted(response_json.keys()),
            }
        )
        records.extend(announcements)
        if not has_more:
            break
    else:
        stopped_by_page_limit = True

    sample = records[0] if records else {}
    sample_url = normalize_announcement_url(sample.get("adjunctUrl", ""))
    return {
        "probe": "empty_searchkey" if include_searchkey else "without_searchkey",
        "date": day,
        "column": column,
        "max_pages": max_pages,
        "record_count": len(records),
        "stopped_by_page_limit": stopped_by_page_limit,
        "pages": pages,
        "sample_keys": sorted(sample.keys()),
        "sample_title": sample.get("announcementTitle", ""),
        "sample_publish_time": sample.get("announcementTime", ""),
        "sample_stock_code": sample.get("secCode", ""),
        "sample_stock_name": sample.get("secName", ""),
        "sample_adjunct_url": sample.get("adjunctUrl", ""),
        "sample_announcement_url": sample_url,
        "sample_announcement_id": extract_announcement_id(
            sample.get("announcementId"),
            sample.get("announcement_id"),
            sample.get("adjunctUrl"),
            sample_url,
        ),
    }


def print_probe_summary(summary: dict[str, Any]) -> None:
    print("=" * 88)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


def run_probe(day: str, max_pages: int, column: str) -> int:
    service = CninfoCrawlerService()
    for include_searchkey in (True, False):
        summary = probe_variant(
            service=service,
            day=day,
            max_pages=max_pages,
            column=column,
            include_searchkey=include_searchkey,
        )
        print_probe_summary(summary)
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
