"""Daily/backfill crawl watermark management."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STATE_PATH = Path("data/state/crawl_state.json")


def default_state() -> dict[str, Any]:
    today = date.today()
    return {
        "version": 1,
        "source": "cninfo",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "daily": {
            "hot_window_days": 3,
            "verify_window_days": 14,
            "last_success_at": None,
            "last_success_range": None,
        },
        "backfill": {
            "target_start_date": (today - timedelta(days=365 * 3)).strftime("%Y-%m-%d"),
            "target_end_date": today.strftime("%Y-%m-%d"),
            "direction": "backward",
            "next_end_date": today.strftime("%Y-%m-%d"),
            "batch_days": 7,
            "completed_ranges": [],
            "failed_dates": [],
        },
        "limits": {
            "max_days_per_run": 7,
            "max_pages_per_day": 3000,
            "max_pages_per_run": 8000,
            "max_runtime_minutes": 55,
        },
        "date_status": {},
    }


def load_state(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return default_state()
    with path.open("r", encoding="utf-8") as file:
        state = json.load(file)
    base = default_state()
    for key, value in base.items():
        state.setdefault(key, value)
    return state


def save_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    with path.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2, sort_keys=True)
        file.write("\n")


def date_range(start: date, end: date) -> list[str]:
    if start > end:
        return []
    days = (end - start).days
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days + 1)]


def resolve_dates(state: dict[str, Any], mode: str, hot_window_days: int, verify_window_days: int, batch_days: int) -> list[str]:
    today = date.today()
    if mode == "daily":
        window = max(1, hot_window_days)
        return date_range(today - timedelta(days=window - 1), today)
    if mode == "verify":
        window = max(1, verify_window_days)
        return date_range(today - timedelta(days=window - 1), today)
    if mode == "backfill":
        backfill = state.setdefault("backfill", default_state()["backfill"])
        next_end = datetime.strptime(backfill.get("next_end_date") or today.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
        target_start = datetime.strptime(backfill.get("target_start_date"), "%Y-%m-%d").date()
        batch = max(1, batch_days)
        start = max(target_start, next_end - timedelta(days=batch - 1))
        return date_range(start, next_end)
    raise ValueError(f"Unsupported mode: {mode}")


def mark_date_status(state: dict[str, Any], day: str, status: str, detail: dict[str, Any]) -> None:
    state.setdefault("date_status", {})[day] = {
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **detail,
    }


def advance_backfill_if_success(state: dict[str, Any], dates: list[str]) -> None:
    if not dates:
        return
    backfill = state.setdefault("backfill", default_state()["backfill"])
    failed = [day for day in dates if state.get("date_status", {}).get(day, {}).get("status") != "completed"]
    if failed:
        current_failed = set(backfill.get("failed_dates", []))
        current_failed.update(failed)
        backfill["failed_dates"] = sorted(current_failed)
        return
    start = min(dates)
    end = max(dates)
    backfill.setdefault("completed_ranges", []).append({"start": start, "end": end})
    next_end = datetime.strptime(start, "%Y-%m-%d").date() - timedelta(days=1)
    target_start = datetime.strptime(backfill.get("target_start_date"), "%Y-%m-%d").date()
    if next_end >= target_start:
        backfill["next_end_date"] = next_end.strftime("%Y-%m-%d")
