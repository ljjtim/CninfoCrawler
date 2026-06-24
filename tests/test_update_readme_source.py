import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from cninfo_models import FILTERED_COLUMNS, LEGACY_COLUMNS
from update_readme import update_readme


class UpdateReadmeSourceTest(unittest.TestCase):
    def test_update_readme_prefers_latest_filtered_export(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                Path("exports").mkdir()
                pd.DataFrame(
                    [
                        {
                            "announcement_id": "1225384530",
                            "keyword": "警示",
                            "matched_field": "title",
                            "publish_date": "2026-06-24",
                            "stock_code": "600735",
                            "stock_name": "ST新华锦",
                            "title": "新华锦关于公司股票被实施其他风险警示事项的进展公告",
                            "announcement_url": "http://static.cninfo.com.cn/finalpage/2026-06-24/1225384530.PDF",
                        }
                    ],
                    columns=FILTERED_COLUMNS,
                ).to_csv("exports/latest_7d_filtered.csv", index=False, encoding="utf-8-sig")
                pd.DataFrame(
                    [
                        {
                            "keyword": "退市",
                            "stock_code": "000001",
                            "stock_name": "旧数据",
                            "title": "旧 announcements 数据不应出现在 README",
                            "publish_time": "2026-06-24",
                            "announcement_url": "http://example.com/old.pdf",
                        }
                    ],
                    columns=LEGACY_COLUMNS,
                ).to_csv("announcements.csv", index=False, encoding="utf-8-sig")

                update_readme()

                readme = Path("README.md").read_text(encoding="utf-8")
                self.assertIn("ST新华锦", readme)
                self.assertNotIn("旧 announcements 数据不应出现在 README", readme)
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
