import logging
from cninfo_service import CninfoCrawlerService, CrawlConfig, DEFAULT_KEYWORDS, get_default_date_range

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main() -> int:
    start_date, end_date = get_default_date_range(days=7)
    config = CrawlConfig(
        start_date=start_date,
        end_date=end_date,
        keywords=DEFAULT_KEYWORDS,
        output_file="announcements.csv",
        force_update=False,
        delay_range=(1, 3),
        proxies=None,
    )
    crawler = CninfoCrawlerService()
    logging.info("开始执行任务。滚动日期范围: %s 至 %s", start_date, end_date)
    new_records = crawler.run_incremental_update(config)
    logging.info("任务完成！本次共新增记录: %s 条", len(new_records))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
