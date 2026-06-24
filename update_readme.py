from __future__ import annotations

from pathlib import Path

from cninfo_service import build_readme_content


def resolve_readme_source() -> str:
    preferred = Path("exports/latest_7d_filtered.csv")
    if preferred.exists() and preferred.stat().st_size > 0:
        return preferred.as_posix()
    return "announcements.csv"


def update_readme() -> None:
    source = resolve_readme_source()
    # Keep the legacy README renderer for now. filter_announcements.py still writes
    # announcements.csv in the old schema, so README generation remains compatible.
    readme_content = build_readme_content(csv_file="announcements.csv", days=7)
    if source != "announcements.csv":
        readme_content = readme_content.replace(
            "自动提取近 7 天的关键词监控公告。",
            "自动提取近 7 天本地关键词过滤公告。全量公告元数据保存在 data/raw/，过滤结果保存在 exports/。",
        )
    with open("README.md", "w", encoding="utf-8") as file:
        file.write(readme_content)


if __name__ == "__main__":
    update_readme()
