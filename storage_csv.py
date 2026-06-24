"""CSV storage helpers for full Cninfo announcement metadata."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from cninfo_models import FILTERED_COLUMNS, LEGACY_COLUMNS, RAW_COLUMNS, clean_text, normalize_stock_code

RAW_ROOT = Path("data/raw")
EXPORTS_ROOT = Path("exports")


def raw_month_path(publish_date: str) -> Path:
    dt = datetime.strptime(publish_date, "%Y-%m-%d")
    return RAW_ROOT / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"announcements_{dt.year:04d}-{dt.month:02d}.csv"


def normalize_dataframe_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    working = df.copy()
    for column in columns:
        if column not in working.columns:
            working[column] = ""
    working = working[columns].fillna("")
    if "stock_code" in working.columns:
        working["stock_code"] = working["stock_code"].map(normalize_stock_code)
    return working


def read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)
    return normalize_dataframe_columns(df, columns)


def write_csv(path: Path, df: pd.DataFrame, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    working = normalize_dataframe_columns(df, columns)
    working.to_csv(path, index=False, encoding="utf-8-sig")


def append_csv_rows(path: Path, df: pd.DataFrame, columns: list[str]) -> int:
    if df.empty:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    working = normalize_dataframe_columns(df, columns)
    file_exists = path.exists() and path.stat().st_size > 0
    working.to_csv(path, mode="a", index=False, header=not file_exists, encoding="utf-8-sig")
    return len(working)


def merge_raw_records(records: Iterable[Mapping[str, str]]) -> dict[str, int]:
    grouped: dict[Path, list[dict[str, str]]] = {}
    skipped = 0
    for record in records:
        normalized = {column: clean_text(record.get(column, "")) for column in RAW_COLUMNS}
        normalized["stock_code"] = normalize_stock_code(normalized.get("stock_code", ""))
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
    working = normalize_dataframe_columns(filtered_df, FILTERED_COLUMNS)
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


def filtered_to_legacy(filtered_df: pd.DataFrame) -> pd.DataFrame:
    if filtered_df.empty:
        return pd.DataFrame(columns=LEGACY_COLUMNS)
    return pd.DataFrame({
        "keyword": filtered_df.get("keyword", ""),
        "stock_code": filtered_df.get("stock_code", "").map(normalize_stock_code),
        "stock_name": filtered_df.get("stock_name", ""),
        "title": filtered_df.get("title", ""),
        "publish_time": filtered_df.get("publish_date", ""),
        "announcement_url": filtered_df.get("announcement_url", ""),
    })


def write_legacy_announcements(filtered_df: pd.DataFrame, output_file: str = "announcements.csv", preserve_existing: bool = True) -> None:
    path = Path(output_file)
    new_legacy = filtered_to_legacy(filtered_df)
    if new_legacy.empty:
        return

    if not preserve_existing or not path.exists() or path.stat().st_size == 0:
        new_legacy = new_legacy.drop_duplicates(subset=["keyword", "announcement_url"], keep="last")
        new_legacy = new_legacy.sort_values(by=["publish_time", "stock_code", "keyword", "title"], ascending=[False, True, True, True])
        write_csv(path, new_legacy, LEGACY_COLUMNS)
        return

    existing = read_csv_or_empty(path, LEGACY_COLUMNS)
    existing_ids = set((existing["keyword"].astype(str) + "|" + existing["announcement_url"].astype(str)).tolist())
    candidate = new_legacy.drop_duplicates(subset=["keyword", "announcement_url"], keep="last")
    candidate_ids = candidate["keyword"].astype(str) + "|" + candidate["announcement_url"].astype(str)
    to_append = candidate.loc[~candidate_ids.isin(existing_ids)].copy()
    append_csv_rows(path, to_append, LEGACY_COLUMNS)
