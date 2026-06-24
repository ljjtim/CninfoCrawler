"""CSV storage helpers for full Cninfo announcement metadata."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from cninfo_models import FILTERED_COLUMNS, LEGACY_COLUMNS, RAW_COLUMNS, clean_text

RAW_ROOT = Path("data/raw")
EXPORTS_ROOT = Path("exports")


def raw_month_path(publish_date: str) -> Path:
    dt = datetime.strptime(publish_date, "%Y-%m-%d")
    return RAW_ROOT / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"announcements_{dt.year:04d}-{dt.month:02d}.csv"


def read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df[columns].fillna("")


def write_csv(path: Path, df: pd.DataFrame, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    working = df.copy()
    for column in columns:
        if column not in working.columns:
            working[column] = ""
    working = working[columns].fillna("")
    working.to_csv(path, index=False, encoding="utf-8-sig")


def merge_raw_records(records: Iterable[Mapping[str, str]]) -> dict[str, int]:
    grouped: dict[Path, list[dict[str, str]]] = {}
    skipped = 0
    for record in records:
        normalized = {column: clean_text(record.get(column, "")) for column in RAW_COLUMNS}
        publish_date = normalized.get("publish_date", "")
        if not publish_date:
            skipped += 1
            continue
        grouped.setdefault(raw_month_path(publish_date), []).append(normalized)

    written = 0
    added = 0
    for path, month_records in grouped.items():
        old_df = read_csv_or_empty(path, RAW_COLUMNS)
        old_count = len(old_df)
        new_df = pd.DataFrame(month_records, columns=RAW_COLUMNS)
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["announcement_id"], keep="last")
        merged = merged.sort_values(by=["publish_date", "stock_code", "title"], ascending=[False, True, True])
        write_csv(path, merged, RAW_COLUMNS)
        written += len(month_records)
        added += max(len(merged) - old_count, 0)
    return {"input": written + skipped, "written": written, "added": added, "skipped": skipped}


def iter_raw_files(raw_root: Path = RAW_ROOT) -> list[Path]:
    if not raw_root.exists():
        return []
    return sorted(raw_root.glob("year=*/month=*/announcements_*.csv"))


def load_all_raw(raw_root: Path = RAW_ROOT) -> pd.DataFrame:
    frames = [read_csv_or_empty(path, RAW_COLUMNS) for path in iter_raw_files(raw_root)]
    if not frames:
        return pd.DataFrame(columns=RAW_COLUMNS)
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["announcement_id"], keep="last")


def write_filtered_exports(filtered_df: pd.DataFrame) -> None:
    EXPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    working = filtered_df.copy()
    for column in FILTERED_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    working = working[FILTERED_COLUMNS].fillna("")
    if not working.empty:
        working = working.drop_duplicates(subset=["announcement_id", "keyword"], keep="last")
        working = working.sort_values(by=["publish_date", "stock_code", "keyword", "title"], ascending=[False, True, True, True])
    write_csv(EXPORTS_ROOT / "filtered_announcements.csv", working, FILTERED_COLUMNS)

    today = datetime.now().date()
    for days, name in ((7, "latest_7d_filtered.csv"), (30, "latest_30d_filtered.csv")):
        if working.empty:
            window_df = working.copy()
        else:
            dates = pd.to_datetime(working["publish_date"], errors="coerce").dt.date
            threshold = today - timedelta(days=days)
            window_df = working.loc[dates >= threshold].copy()
        write_csv(EXPORTS_ROOT / name, window_df, FILTERED_COLUMNS)


def write_legacy_announcements(filtered_df: pd.DataFrame, output_file: str = "announcements.csv") -> None:
    legacy = pd.DataFrame(columns=LEGACY_COLUMNS)
    if not filtered_df.empty:
        legacy = pd.DataFrame({
            "keyword": filtered_df.get("keyword", ""),
            "stock_code": filtered_df.get("stock_code", ""),
            "stock_name": filtered_df.get("stock_name", ""),
            "title": filtered_df.get("title", ""),
            "publish_time": filtered_df.get("publish_date", ""),
            "announcement_url": filtered_df.get("announcement_url", ""),
        })
        legacy = legacy.drop_duplicates(subset=["keyword", "announcement_url"], keep="last")
        legacy = legacy.sort_values(by=["publish_time", "stock_code", "keyword", "title"], ascending=[False, True, True, True])
    write_csv(Path(output_file), legacy, LEGACY_COLUMNS)
