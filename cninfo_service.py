import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd
import requests

from cninfo_models import normalize_stock_code

DEFAULT_KEYWORDS = ["警示", "责令改正", "行政监管", "立案", "行政处罚", "退市"]
DEFAULT_OUTPUT_FILE = "announcements.csv"
CSV_COLUMNS = [
    "keyword",
    "stock_code",
    "stock_name",
    "title",
    "publish_time",
    "announcement_url",
]

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CrawlConfig:
    start_date: str
    end_date: str
    keywords: Sequence[str] = field(default_factory=lambda: list(DEFAULT_KEYWORDS))
    output_file: str = DEFAULT_OUTPUT_FILE
    force_update: bool = False
    delay_range: Tuple[float, float] = (1, 3)
    proxies: Optional[Dict[str, str]] = None
    page_size: int = 30
    column: str = "szse"
    tab_name: str = "fulltext"
    sort_name: str = "pubdate"
    sort_type: str = "desc"
    request_timeout: int = 15


def get_default_date_range(days: int = 7) -> Tuple[str, str]:
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return start, end


def split_date_range_by_month(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    start_dt = datetime.strptime(normalize_date_str(start_date), "%Y-%m-%d")
    end_dt = datetime.strptime(normalize_date_str(end_date), "%Y-%m-%d")
    ranges: List[Tuple[str, str]] = []
    cursor = start_dt

    while cursor <= end_dt:
        next_month = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end_dt)
        ranges.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end + timedelta(days=1)

    return ranges


def empty_announcements_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CSV_COLUMNS)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("<em>", "").replace("</em>", "").strip()


def normalize_date_str(value: Any) -> str:
    if value is None or value == "":
        return ""

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return ""

    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def build_announcement_url(adjunct_url: Any) -> str:
    cleaned = clean_text(adjunct_url).lstrip("/")
    if not cleaned:
        return ""
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return cleaned
    return f"http://static.cninfo.com.cn/{cleaned}"


def normalize_record(record: Mapping[str, Any]) -> Dict[str, str]:
    normalized = {
        "keyword": clean_text(record.get("keyword")),
        "stock_code": normalize_stock_code(record.get("stock_code")),
        "stock_name": clean_text(record.get("stock_name")),
        "title": clean_text(record.get("title")),
        "publish_time": normalize_date_str(record.get("publish_time")),
        "announcement_url": build_announcement_url(record.get("announcement_url")),
    }
    return normalized


def summarize_record(record: Mapping[str, Any], title_limit: int = 80) -> str:
    normalized = normalize_record(record)
    title = normalized["title"]
    if len(title) > title_limit:
        title = f"{title[: title_limit - 3]}..."
    return (
        f"日期={normalized['publish_time']} "
        f"代码={normalized['stock_code']} "
        f"简称={normalized['stock_name']} "
        f"标题={title}"
    )


def record_unique_id(record: Mapping[str, Any]) -> str:
    normalized = normalize_record(record)
    keyword = normalized["keyword"]
    announcement_url = normalized["announcement_url"]
    if announcement_url:
        return f"{keyword}|{announcement_url}"
    return "|".join(
        [
            keyword,
            normalized["stock_code"],
            normalized["title"],
            normalized["publish_time"],
        ]
    )


def parse_announcement(item: Mapping[str, Any], keyword: str) -> Optional[Dict[str, str]]:
    title = clean_text(item.get("announcementTitle"))
    announcement_time = item.get("announcementTime")
    publish_time = ""
    if announcement_time not in (None, ""):
        try:
            publish_time = datetime.fromtimestamp(float(announcement_time) / 1000).strftime("%Y-%m-%d")
        except (TypeError, ValueError, OSError):
            LOGGER.warning("跳过异常公告时间数据: %s", announcement_time)

    record = normalize_record(
        {
            "keyword": keyword,
            "stock_code": item.get("secCode"),
            "stock_name": item.get("secName"),
            "title": title,
            "publish_time": publish_time,
            "announcement_url": item.get("adjunctUrl"),
        }
    )

    if not record["title"] or not record["publish_time"]:
        LOGGER.warning("跳过缺少关键字段的公告数据: title=%s, publish_time=%s", record["title"], record["publish_time"])
        return None

    return record


def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    for column in CSV_COLUMNS:
        if column not in working.columns:
            working[column] = ""

    working = working[CSV_COLUMNS].fillna("")
    for column in CSV_COLUMNS:
        working[column] = working[column].map(clean_text)
    working["stock_code"] = working["stock_code"].map(normalize_stock_code)
    working["publish_time"] = working["publish_time"].map(normalize_date_str)
    working["announcement_url"] = working["announcement_url"].map(build_announcement_url)
    return working


def load_announcements_dataframe(csv_file: str = DEFAULT_OUTPUT_FILE) -> pd.DataFrame:
    if not os.path.exists(csv_file):
        return empty_announcements_frame()

    encodings = ("utf-8-sig", "utf-8")
    last_error: Optional[Exception] = None
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, encoding=encoding)
            return standardize_dataframe(df)
        except pd.errors.EmptyDataError:
            LOGGER.warning("CSV 文件为空: %s", csv_file)
            return empty_announcements_frame()
        except Exception as exc:
            last_error = exc

    LOGGER.error("读取 CSV 失败，将按空数据处理: %s", last_error)
    return empty_announcements_frame()


def append_records_to_csv(records: Iterable[Mapping[str, Any]], csv_file: str = DEFAULT_OUTPUT_FILE) -> int:
    normalized_records = [normalize_record(record) for record in records]
    if not normalized_records:
        return 0

    df = standardize_dataframe(pd.DataFrame(normalized_records))
    file_exists = os.path.isfile(csv_file) and os.path.getsize(csv_file) > 0
    df.to_csv(csv_file, mode="a", index=False, header=not file_exists, encoding="utf-8-sig")
    return len(df)


def get_existing_unique_ids(csv_file: str = DEFAULT_OUTPUT_FILE) -> Set[str]:
    df = load_announcements_dataframe(csv_file)
    if df.empty:
        return set()
    return {record_unique_id(record) for record in df.to_dict("records")}


def filter_records_by_existing_ids(records: Iterable[Mapping[str, Any]], existing_ids: Optional[Set[str]] = None) -> List[Dict[str, str]]:
    seen_ids = set(existing_ids or set())
    new_records: List[Dict[str, str]] = []

    for record in records:
        normalized = normalize_record(record)
        uid = record_unique_id(normalized)
        if uid in seen_ids:
            continue
        seen_ids.add(uid)
        new_records.append(normalized)

    return new_records


def filter_dataframe_by_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    working = standardize_dataframe(df)
    publish_time_series = pd.to_datetime(working["publish_time"], errors="coerce")
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    mask = publish_time_series.between(start_ts, end_ts, inclusive="both")
    return working.loc[mask].copy()


def get_csv_date_bounds(csv_file: str = DEFAULT_OUTPUT_FILE) -> Tuple[Optional[str], Optional[str]]:
    df = load_announcements_dataframe(csv_file)
    if df.empty:
        return None, None

    publish_time_series = pd.to_datetime(df["publish_time"], errors="coerce").dropna()
    if publish_time_series.empty:
        return None, None

    return publish_time_series.min().strftime("%Y-%m-%d"), publish_time_series.max().strftime("%Y-%m-%d")


def build_readme_content_from_dataframe(
    df: pd.DataFrame,
    days: int = 7,
    description: str = "自动提取近 7 天的关键词监控公告。",
) -> str:
    df = standardize_dataframe(df)
    if df.empty:
        table_content = "当前暂无公告数据。"
    else:
        publish_time_series = pd.to_datetime(df["publish_time"], errors="coerce")
        threshold = datetime.now() - timedelta(days=days)
        recent_df = df.loc[publish_time_series >= threshold].copy()
        recent_df = recent_df.sort_values(by=["publish_time", "stock_code", "title"], ascending=[False, True, True])

        if recent_df.empty:
            table_content = "最近 7 天暂无匹配公告。"
        else:
            recent_df["title"] = recent_df.apply(
                lambda row: f"[{row['title']}]({row['announcement_url']})"
                if row["announcement_url"]
                else row["title"],
                axis=1,
            )
            display_df = recent_df[["publish_time", "stock_code", "stock_name", "keyword", "title"]].copy()
            display_df.columns = ["发布日期", "代码", "简称", "关键字", "公告标题 (点击跳转)"]
            table_content = display_df.to_markdown(index=False)

    return f"""# 巨潮资讯公告监控 (CninfoCrawler)

> {description}更新时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

{table_content}

## 完整性校验使用示例

> `README.md` 由脚本自动生成，请优先修改 `update_readme.py` 或 `cninfo_service.py` 中的模板，避免被 GitHub Actions 覆盖。

- 校验指定时间范围，程序会自动按月份拆分抓取：
  - `python verify_csv_integrity.py --start-date 2022-01-01 --end-date 2022-12-31`
- 发现遗漏后追加补齐到 `announcements.csv` 末尾：
  - `python verify_csv_integrity.py --start-date 2022-01-01 --end-date 2022-12-31 --repair`
- 如果时间跨度较大，建议先不带 `--repair` 观察日志和缺失统计，再决定是否补写。

---
*更多历史数据请查看 [announcements.csv](./announcements.csv)*
"""


def build_readme_content(csv_file: str = DEFAULT_OUTPUT_FILE, days: int = 7) -> str:
    df = load_announcements_dataframe(csv_file)
    return build_readme_content_from_dataframe(df, days=days)


class CninfoCrawlerService:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

    def _fetch_keyword_records_for_range(self, keyword: str, start_date: str, end_date: str, config: CrawlConfig) -> List[Dict[str, str]]:
        page = 1
        all_records: List[Dict[str, str]] = []
        seen_page_signatures: Set[Tuple[str, ...]] = set()

        while True:
            LOGGER.info(
                "开始抓取关键词 [%s] 第 %s 页，日期范围 %s ~ %s，当前已抓取 %s 条。",
                keyword,
                page,
                start_date,
                end_date,
                len(all_records),
            )
            payload = {
                "pageNum": page,
                "pageSize": config.page_size,
                "column": config.column,
                "tabName": config.tab_name,
                "searchkey": keyword,
                "seDate": f"{start_date}~{end_date}",
                "sortName": config.sort_name,
                "sortType": config.sort_type,
                "isHLtitle": "true",
            }

            try:
                response = self.session.post(
                    self.url,
                    data=payload,
                    headers=self.headers,
                    proxies=config.proxies,
                    timeout=config.request_timeout,
                )
                response.raise_for_status()
                response_json = response.json()
            except Exception as exc:
                LOGGER.error("请求失败 (%s - 第%s页): %s", keyword, page, exc)
                break

            announcements = response_json.get("announcements", [])
            if not announcements:
                LOGGER.info("关键词 [%s] 第 %s 页无数据，结束该关键词抓取。", keyword, page)
                break

            LOGGER.info("关键词 [%s] 第 %s 页返回 %s 条原始记录。", keyword, page, len(announcements))

            page_records: List[Dict[str, str]] = []
            for item in announcements:
                record = parse_announcement(item, keyword)
                if record:
                    page_records.append(record)

            if page_records:
                page_signature = tuple(record_unique_id(record) for record in page_records)
                if page_signature in seen_page_signatures:
                    LOGGER.warning(
                        "关键词 [%s] 在 %s ~ %s 的第 %s 页出现重复分页结果，疑似接口分页回卷，提前停止该分片抓取。",
                        keyword,
                        start_date,
                        end_date,
                        page,
                    )
                    break
                seen_page_signatures.add(page_signature)
                LOGGER.info(
                    "关键词 [%s] 第 %s 页第一条数据：%s",
                    keyword,
                    page,
                    summarize_record(page_records[0]),
                )
                all_records.extend(page_records)
                LOGGER.info("关键词 [%s] 抓取累计有效记录 %s 条。", keyword, len(all_records))
            else:
                LOGGER.warning("关键词 [%s] 第 %s 页记录均被过滤，未产生有效数据。", keyword, page)

            if not response_json.get("hasMore", False):
                LOGGER.info("关键词 [%s] 已抓取完毕，共 %s 条有效记录。", keyword, len(all_records))
                break

            page += 1
            time.sleep(random.uniform(*config.delay_range))

        return all_records

    def fetch_keyword_records(self, keyword: str, start_date: str, end_date: str, config: CrawlConfig) -> List[Dict[str, str]]:
        monthly_ranges = split_date_range_by_month(start_date, end_date)
        if len(monthly_ranges) > 1:
            LOGGER.info(
                "关键词 [%s] 的日期范围 %s ~ %s 已自动拆分为 %s 个月份分片抓取。",
                keyword,
                start_date,
                end_date,
                len(monthly_ranges),
            )

        all_records: List[Dict[str, str]] = []
        seen_ids: Set[str] = set()
        for index, (chunk_start, chunk_end) in enumerate(monthly_ranges, start=1):
            LOGGER.info(
                "关键词 [%s] 开始抓取第 %s/%s 个分片：%s ~ %s。",
                keyword,
                index,
                len(monthly_ranges),
                chunk_start,
                chunk_end,
            )
            chunk_records = self._fetch_keyword_records_for_range(keyword, chunk_start, chunk_end, config)
            for record in chunk_records:
                uid = record_unique_id(record)
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)
                all_records.append(record)
            LOGGER.info(
                "关键词 [%s] 第 %s/%s 个分片抓取完成，当前关键词累计 %s 条唯一记录。",
                keyword,
                index,
                len(monthly_ranges),
                len(all_records),
            )

        return all_records

    def fetch_records(self, config: CrawlConfig) -> List[Dict[str, str]]:
        all_records: List[Dict[str, str]] = []
        seen_ids: Set[str] = set()

        for keyword in config.keywords:
            LOGGER.info("开始处理关键词 [%s]。", keyword)
            keyword_records = self.fetch_keyword_records(keyword, config.start_date, config.end_date, config)
            for record in keyword_records:
                uid = record_unique_id(record)
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)
                all_records.append(record)
            LOGGER.info("关键词 [%s] 处理完成，当前跨关键词累计 %s 条唯一记录。", keyword, len(all_records))

        return all_records

    def run_incremental_update(self, config: CrawlConfig) -> List[Dict[str, str]]:
        fetched_records = self.fetch_records(config)
        if config.force_update:
            new_records = fetched_records
        else:
            existing_ids = get_existing_unique_ids(config.output_file)
            new_records = filter_records_by_existing_ids(fetched_records, existing_ids)

        saved_count = append_records_to_csv(new_records, config.output_file)
        LOGGER.info("成功保存 %s 条新记录至 %s", saved_count, config.output_file)
        return new_records
