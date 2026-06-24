import tempfile
import unittest
from pathlib import Path

import pandas as pd

from cninfo_service import (
    CSV_COLUMNS,
    append_records_to_csv,
    load_announcements_dataframe,
    normalize_record,
)


class StockCodeNormalizationTest(unittest.TestCase):
    def test_normalize_record_pads_plain_numeric_stock_code(self):
        record = normalize_record(
            {
                "keyword": "退市",
                "stock_code": "2868",
                "stock_name": "测试公司",
                "title": "关于申请撤销退市风险警示的公告",
                "publish_time": "2026-06-24",
                "announcement_url": "finalpage/2026-06-24/1234567890.PDF",
            }
        )

        self.assertEqual(record["stock_code"], "002868")

    def test_legacy_csv_read_and_append_keep_six_digit_stock_code(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "announcements.csv"
            pd.DataFrame(
                [
                    {
                        "keyword": "警示",
                        "stock_code": "615",
                        "stock_name": "测试公司",
                        "title": "关于申请撤销部分退市风险警示的公告",
                        "publish_time": "2026-06-24",
                        "announcement_url": "http://static.cninfo.com.cn/finalpage/2026-06-24/1234567890.PDF",
                    }
                ],
                columns=CSV_COLUMNS,
            ).to_csv(csv_path, index=False, encoding="utf-8-sig")

            loaded = load_announcements_dataframe(str(csv_path))
            self.assertEqual(loaded.loc[0, "stock_code"], "000615")

            append_records_to_csv(
                [
                    {
                        "keyword": "退市",
                        "stock_code": "2868",
                        "stock_name": "测试公司",
                        "title": "关于申请撤销退市风险警示的公告",
                        "publish_time": "2026-06-24",
                        "announcement_url": "finalpage/2026-06-24/2234567890.PDF",
                    }
                ],
                str(csv_path),
            )

            appended = load_announcements_dataframe(str(csv_path))
            self.assertIn("002868", appended["stock_code"].tolist())


if __name__ == "__main__":
    unittest.main()
