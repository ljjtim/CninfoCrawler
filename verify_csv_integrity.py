import argparse
import logging
from datetime import datetime
from typing import List, Optional, Sequence

from cninfo_service import (
    CninfoCrawlerService,
    CrawlConfig,
    DEFAULT_KEYWORDS,
    DEFAULT_OUTPUT_FILE,
    filter_dataframe_by_date_range,
    get_csv_date_bounds,
    get_default_date_range,
    load_announcements_dataframe,
    record_unique_id,
    append_records_to_csv,
    summarize_record,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="重新抓取并校验 announcements.csv 是否存在遗漏记录。")
    parser.add_argument("--start-date", help="校验开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end-date", help="校验结束日期，格式 YYYY-MM-DD")
    parser.add_argument("--csv-file", default=DEFAULT_OUTPUT_FILE, help="待校验的 CSV 文件路径")
    parser.add_argument("--repair", action="store_true", help="发现遗漏时，将缺失记录追加到 CSV 末尾")
    return parser.parse_args()


def resolve_date_range(csv_file: str, start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
    if start_date and end_date:
        return start_date, end_date

    csv_start, csv_end = get_csv_date_bounds(csv_file)
    if csv_start and csv_end:
        return start_date or csv_start, end_date or csv_end

    fallback_start, fallback_end = get_default_date_range(days=7)
    logging.warning("未能从 %s 推断日期范围，回退到最近 7 天: %s 至 %s", csv_file, fallback_start, fallback_end)
    return start_date or fallback_start, end_date or fallback_end


def warn_if_large_range(start_date: str, end_date: str) -> None:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    range_days = (end_dt - start_dt).days + 1
    if range_days > 366:
        logging.warning("当前校验区间共 %s 天，任务可能耗时较长，建议按年或按季度分段执行。", range_days)


def compare_missing_records(csv_file: str, fetched_records: Sequence[dict], start_date: str, end_date: str) -> List[dict]:
    logging.info("正在加载本地 CSV 并筛选指定日期范围的数据...")
    local_df = load_announcements_dataframe(csv_file)
    local_df = filter_dataframe_by_date_range(local_df, start_date, end_date)
    local_ids = {record_unique_id(record) for record in local_df.to_dict("records")}
    logging.info("本地范围内共有 %s 条记录，开始与重抓结果比对。", len(local_ids))

    missing_records: List[dict] = []
    for record in fetched_records:
        if record_unique_id(record) not in local_ids:
            missing_records.append(record)
    return missing_records


def print_missing_preview(missing_records: Sequence[dict]) -> None:
    if not missing_records:
        logging.info("校验完成，CSV 没有遗漏记录。")
        return

    logging.warning("发现 %s 条遗漏记录，预览前 10 条：", len(missing_records))
    for record in list(missing_records)[:10]:
        logging.warning("遗漏记录: %s 关键词=%s", summarize_record(record), record.get("keyword", ""))


def main() -> int:
    args = parse_args()
    start_date, end_date = resolve_date_range(args.csv_file, args.start_date, args.end_date)
    warn_if_large_range(start_date, end_date)
    config = CrawlConfig(
        start_date=start_date,
        end_date=end_date,
        keywords=DEFAULT_KEYWORDS,
        output_file=args.csv_file,
        force_update=True,
        delay_range=(1, 3),
        proxies=None,
    )

    logging.info("开始校验 CSV 完整性。")
    logging.info("参数：csv=%s，日期范围=%s 至 %s，repair=%s", args.csv_file, start_date, end_date, args.repair)
    logging.info("建议在终端中使用 `python verify_csv_integrity.py ...` 执行，避免关联启动后窗口自动关闭。")
    crawler = CninfoCrawlerService()
    logging.info("开始重新抓取远端公告数据，请耐心等待。")
    fetched_records = crawler.fetch_records(config)
    logging.info("远端重抓完成，共获得 %s 条唯一记录。", len(fetched_records))
    logging.info("开始比对本地 CSV 是否存在遗漏...")
    missing_records = compare_missing_records(args.csv_file, fetched_records, start_date, end_date)

    local_df = filter_dataframe_by_date_range(load_announcements_dataframe(args.csv_file), start_date, end_date)
    logging.info("缺失统计完成。")
    logging.info("本地范围内记录数: %s", len(local_df))
    logging.info("远端重抓记录数: %s", len(fetched_records))
    logging.info("缺失记录数: %s", len(missing_records))
    print_missing_preview(missing_records)

    if missing_records and args.repair:
        logging.warning("即将把遗漏记录追加到 %s 末尾。", args.csv_file)
        appended_count = append_records_to_csv(missing_records, args.csv_file)
        logging.warning("已将 %s 条遗漏记录追加写入 %s 末尾，请确认结果。", appended_count, args.csv_file)
    elif missing_records:
        logging.warning(
            "如需补齐，请重新执行: python verify_csv_integrity.py --start-date %s --end-date %s --repair",
            start_date,
            end_date,
        )
    else:
        logging.info("未发现遗漏，无需补写 CSV。")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
