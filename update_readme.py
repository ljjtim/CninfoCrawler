from __future__ import annotations

from pathlib import Path

from cninfo_models import FILTERED_COLUMNS
from cninfo_service import build_readme_content, build_readme_content_from_dataframe
from storage_csv import filtered_to_legacy, read_csv_or_empty


def resolve_readme_source() -> str:
    preferred = Path("exports/latest_7d_filtered.csv")
    if preferred.exists() and preferred.stat().st_size > 0:
        return preferred.as_posix()
    return "announcements.csv"


def update_readme() -> None:
    source = resolve_readme_source()
    if source == "announcements.csv":
        readme_content = build_readme_content(csv_file=source, days=7)
    else:
        filtered_df = read_csv_or_empty(Path(source), FILTERED_COLUMNS)
        legacy_df = filtered_to_legacy(filtered_df)
        readme_content = build_readme_content_from_dataframe(
            legacy_df,
            days=7,
            description="自动提取近 7 天本地关键词过滤公告。全量公告元数据保存在 data/raw/，过滤结果保存在 exports/。",
        )
    with open("README.md", "w", encoding="utf-8") as file:
        file.write(readme_content)


if __name__ == "__main__":
    update_readme()
