import unittest

from cninfo_models import RAW_COLUMNS
from cninfo_service import CninfoCrawlerService, CrawlConfig
from crawl_all_announcements import (
    fetch_all_records_for_day,
    has_run_page_budget,
    page_limit_for_next_date,
)
from probe_cninfo_api import probe_variant


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def post(self, url, data=None, headers=None, proxies=None, timeout=None):
        self.requests.append(
            {
                "url": url,
                "data": dict(data or {}),
                "headers": headers,
                "proxies": proxies,
                "timeout": timeout,
            }
        )
        if not self.responses:
            raise AssertionError("没有配置更多假响应")
        return FakeResponse(self.responses.pop(0))


def sample_item(announcement_id="1225382415", has_category=True):
    item = {
        "announcementId": announcement_id,
        "secCode": "14",
        "secName": "沙河股份",
        "announcementTitle": "关于召开2026年第三次临时股东会的提示性公告",
        "announcementTime": 1782259200000,
        "announcementType": "01010503||010112",
        "adjunctUrl": f"finalpage/2026-06-24/{announcement_id}.PDF",
        "orgId": "gssz0000014",
    }
    if has_category:
        item["announcementTypeName"] = "股东大会"
    return item


class ProbeTest(unittest.TestCase):
    def test_probe_variant_respects_page_limit_and_can_omit_searchkey(self):
        session = FakeSession(
            [
                {"announcements": [sample_item("1225382415")], "hasMore": True},
                {"announcements": [sample_item("1225382416")], "hasMore": True},
            ]
        )
        service = CninfoCrawlerService(session=session)
        summary = probe_variant(
            service=service,
            day="2026-06-24",
            max_pages=2,
            column="szse",
            include_searchkey=False,
        )

        self.assertEqual(summary["record_count"], 2)
        self.assertTrue(summary["stopped_by_page_limit"])
        self.assertEqual(len(session.requests), 2)
        self.assertNotIn("searchkey", session.requests[0]["data"])
        self.assertEqual(session.requests[0]["data"]["pageSize"], 30)
        self.assertEqual(summary["pages"][0]["http_status"], 200)
        self.assertIn("announcementTitle", summary["sample_keys"])
        self.assertEqual(summary["sample_announcement_id"], "1225382415")

    def test_probe_variant_passes_custom_page_size(self):
        session = FakeSession(
            [
                {"announcements": [sample_item("1225382415")], "hasMore": False},
            ]
        )
        service = CninfoCrawlerService(session=session)

        probe_variant(
            service=service,
            day="2026-06-24",
            max_pages=1,
            column="szse",
            include_searchkey=True,
            page_size=50,
        )

        self.assertEqual(session.requests[0]["data"]["pageSize"], 50)


class FullCrawlerTest(unittest.TestCase):
    def test_has_run_page_budget_stops_at_configured_limit(self):
        self.assertTrue(has_run_page_budget(pages_used=7999, max_pages_per_run=8000))
        self.assertFalse(has_run_page_budget(pages_used=8000, max_pages_per_run=8000))
        self.assertTrue(has_run_page_budget(pages_used=8000, max_pages_per_run=0))

    def test_page_limit_for_next_date_uses_remaining_run_budget(self):
        self.assertEqual(page_limit_for_next_date(3000, pages_used=100, max_pages_per_run=8000), 3000)
        self.assertEqual(page_limit_for_next_date(3000, pages_used=7900, max_pages_per_run=8000), 100)
        self.assertEqual(page_limit_for_next_date(3000, pages_used=7900, max_pages_per_run=0), 3000)

    def test_fetch_all_records_for_day_returns_normalized_raw_metadata(self):
        session = FakeSession(
            [
                {"announcements": [sample_item()], "hasMore": False},
            ]
        )
        service = CninfoCrawlerService(session=session)
        config = CrawlConfig(
            start_date="2026-06-24",
            end_date="2026-06-24",
            keywords=[""],
            page_size=30,
            column="szse",
            delay_range=(0, 0),
        )

        records, pages = fetch_all_records_for_day(
            service=service,
            day="2026-06-24",
            config=config,
            max_pages_per_day=10,
            max_retries=0,
            retry_backoffs=[0],
        )

        self.assertEqual(pages, 1)
        self.assertEqual(len(records), 1)
        self.assertEqual(list(records[0].keys()), RAW_COLUMNS)
        self.assertEqual(records[0]["announcement_id"], "1225382415")
        self.assertEqual(records[0]["stock_code"], "000014")
        self.assertEqual(records[0]["publish_date"], "2026-06-24")
        self.assertEqual(records[0]["publish_time_ms"], "1782259200000")
        self.assertEqual(records[0]["category"], "股东大会")
        self.assertEqual(records[0]["org_id"], "gssz0000014")

    def test_fetch_all_records_for_day_uses_announcement_type_when_name_is_missing(self):
        session = FakeSession(
            [
                {"announcements": [sample_item(has_category=False)], "hasMore": False},
            ]
        )
        service = CninfoCrawlerService(session=session)
        config = CrawlConfig(
            start_date="2026-06-24",
            end_date="2026-06-24",
            keywords=[""],
            page_size=30,
            column="szse",
            delay_range=(0, 0),
        )

        records, _ = fetch_all_records_for_day(
            service=service,
            day="2026-06-24",
            config=config,
            max_pages_per_day=10,
            max_retries=0,
            retry_backoffs=[0],
        )

        self.assertEqual(records[0]["category"], "01010503||010112")

    def test_fetch_all_records_for_day_fails_when_empty_page_still_has_more(self):
        session = FakeSession(
            [
                {"announcements": [], "hasMore": True},
            ]
        )
        service = CninfoCrawlerService(session=session)
        config = CrawlConfig(
            start_date="2026-06-24",
            end_date="2026-06-24",
            keywords=[""],
            page_size=30,
            column="szse",
            delay_range=(0, 0),
        )

        with self.assertRaisesRegex(RuntimeError, "hasMore=true"):
            fetch_all_records_for_day(
                service=service,
                day="2026-06-24",
                config=config,
                max_pages_per_day=10,
                max_retries=0,
                retry_backoffs=[0],
            )

    def test_fetch_all_records_for_day_fails_when_page_has_no_valid_records(self):
        item = sample_item()
        item["announcementTime"] = ""
        session = FakeSession(
            [
                {"announcements": [item], "hasMore": False},
            ]
        )
        service = CninfoCrawlerService(session=session)
        config = CrawlConfig(
            start_date="2026-06-24",
            end_date="2026-06-24",
            keywords=[""],
            page_size=30,
            column="szse",
            delay_range=(0, 0),
        )

        with self.assertRaisesRegex(RuntimeError, "no valid records"):
            fetch_all_records_for_day(
                service=service,
                day="2026-06-24",
                config=config,
                max_pages_per_day=10,
                max_retries=0,
                retry_backoffs=[0],
            )


if __name__ == "__main__":
    unittest.main()
