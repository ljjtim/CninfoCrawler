"""按公告文件路径里的日期回填 announcements.csv 的发布日。

背景：早期版本用 datetime.fromtimestamp() 换算公告时间戳，结果跟随本机时区。
GitHub Actions runner 是 UTC，北京时间 00:00 至 08:00 发布的公告会被记成前一天。
本脚本以 finalpage/YYYY-MM-DD/ 目录日期为准重写 publish_time。

用法：
    python fix_publish_dates.py                     # 只统计，不修改
    python fix_publish_dates.py --write             # 写回 CSV
"""

import argparse
import logging
from collections import Counter

import pandas as pd

from cninfo_service import (
    CSV_COLUMNS,
    DEFAULT_OUTPUT_FILE,
    extract_publish_date_from_url,
    load_announcements_dataframe,
    record_unique_id,
    standardize_dataframe,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按公告 URL 目录日期回填 announcements.csv 的发布日。")
    parser.add_argument("--csv-file", default=DEFAULT_OUTPUT_FILE, help="待修正的 CSV 文件路径")
    parser.add_argument("--write", action="store_true", help="把修正结果写回 CSV")
    return parser.parse_args()


def rebuild_publish_time(df: pd.DataFrame) -> tuple[pd.DataFrame, Counter]:
    working = standardize_dataframe(df)
    stats: Counter = Counter()

    for index, row in working.iterrows():
        url_date = extract_publish_date_from_url(row["announcement_url"])
        current = row["publish_time"]
        if not url_date:
            stats["无 URL 日期，保持原值"] += 1
            continue
        if url_date == current:
            stats["日期一致"] += 1
            continue
        working.at[index, "publish_time"] = url_date
        stats["日期已修正"] += 1

    return working, stats


def drop_duplicate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    seen: set[str] = set()
    keep_index = []
    for index, row in df.iterrows():
        uid = record_unique_id(row)
        if uid in seen:
            continue
        seen.add(uid)
        keep_index.append(index)
    removed = len(df) - len(keep_index)
    return df.loc[keep_index].reset_index(drop=True), removed


def main() -> int:
    args = parse_args()
    logging.info("开始校验 %s 的发布日。", args.csv_file)
    df = load_announcements_dataframe(args.csv_file)
    if df.empty:
        logging.warning("%s 没有数据，结束。", args.csv_file)
        return 0

    fixed_df, stats = rebuild_publish_time(df)
    fixed_df, removed = drop_duplicate_rows(fixed_df)

    logging.info("总行数: %s", len(df))
    for key, value in stats.items():
        logging.info("%s: %s", key, value)
    logging.info("去重后删除重复行: %s", removed)
    logging.info("修正后行数: %s", len(fixed_df))

    if not args.write:
        logging.warning("当前为统计模式，未修改文件。如需写入请加 --write")
        return 0

    fixed_df[CSV_COLUMNS].to_csv(args.csv_file, index=False, encoding="utf-8-sig")
    logging.info("已写回 %s。", args.csv_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
