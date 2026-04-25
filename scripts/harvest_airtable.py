#!/usr/bin/env python3
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_CSV = DATA_DIR / "SG_2024_Metrics.csv"
LATEST_CSV = DATA_DIR / "SG_Latest_Airtable.csv"
MERGED_CSV = DATA_DIR / "SG_Merged.csv"
MASTER_HISTORY_CSV = DATA_DIR / "SG_History_Master.csv"
STATE_JSON = DATA_DIR / "SG_Dashboard_State.json"
RAW_SNAPSHOT_DIR = DATA_DIR / "raw_snapshots"
MONTHLY_ARCHIVE_DIR = DATA_DIR / "monthly_archive"
MONTH_LOCKS_JSON = MONTHLY_ARCHIVE_DIR / "_month_locks.json"
LATE_ARRIVALS_CSV = MONTHLY_ARCHIVE_DIR / "_late_arrivals_pending.csv"
DEBUG_DIR = DATA_DIR / "debug"

CANONICAL_HEADERS = [
    "Date",
    "Location",
    "Robot",
    "Item",
    "Version",
    "Robot Time On During Day",
    "Robot Time In Motion During Day",
    "Miles Traveled during day",
    "Utilization",
    "Source",
]

DASHBOARD_FIELDS = [
    "Item",
    "Date",
    "Location",
    "Robot",
    "Version",
    "Miles",
    "OnHours",
    "MotionHours",
    "Utilization",
    "Source",
]


def now_local() -> datetime:
    # GitHub runners use UTC, but the business schedule is Pacific. The workflow cron handles run times.
    return datetime.now(timezone.utc)


def now_str() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S UTC")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    MONTHLY_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def clean_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def canonical_header(value) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()


def compact_header(value) -> str:
    return re.sub(r"[^a-z0-9]", "", clean_text(value).lower())


def canonicalized_row(row: Dict[str, object]) -> Dict[str, object]:
    return {canonical_header(k): v for k, v in (row or {}).items() if canonical_header(k)}


def row_get(row: Dict[str, object], *aliases: str):
    keyed = canonicalized_row(row)
    for alias in aliases:
        val = keyed.get(canonical_header(alias))
        if val not in (None, ""):
            return val
    return None


def parse_date_flexible(date_raw) -> Optional[datetime]:
    if date_raw is None or date_raw == "":
        return None
    if isinstance(date_raw, datetime):
        return date_raw.replace(tzinfo=None)
    if isinstance(date_raw, date):
        return datetime.combine(date_raw, datetime.min.time())

    value = clean_text(date_raw)
    if not value:
        return None

    for fmt in [
        "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y",
        "%Y/%m/%d", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p",
    ]:
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass

    token = value.split()[0].replace(",", "")
    if token and token != value:
        return parse_date_flexible(token)
    return None


def parse_hm(value) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, timedelta):
        return value.total_seconds() / 3600.0
    if isinstance(value, datetime):
        return value.hour + value.minute / 60.0 + value.second / 3600.0
    if isinstance(value, dt_time):
        return value.hour + value.minute / 60.0 + value.second / 3600.0
    if isinstance(value, (int, float)):
        num = float(value)
        # Excel/CSV time fractions like 0.5 mean 12 hours.
        if 0 <= num < 2:
            return num * 24.0
        return num

    text = clean_text(value)
    if not text:
        return 0.0

    for fmt in ["%I:%M:%S %p", "%I:%M %p", "%H:%M:%S", "%H:%M"]:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.hour + parsed.minute / 60.0 + parsed.second / 3600.0
        except Exception:
            pass

    parts = text.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) + int(parts[1]) / 60.0 + int(parts[2]) / 3600.0
        if len(parts) == 2:
            return int(parts[0]) + int(parts[1]) / 60.0
        return float(text.replace(",", "").replace("%", ""))
    except Exception:
        return 0.0


def parse_miles(value) -> float:
    try:
        return float(clean_text(value).replace(",", ""))
    except Exception:
        return 0.0


def normalize_row(raw_row: Dict[str, object], default_source: str = "") -> Optional[Dict[str, object]]:
    dt = parse_date_flexible(row_get(raw_row, "Date"))
    if not dt:
        return None

    robot = clean_text(row_get(raw_row, "Robot"))
    if not robot:
        return None

    miles = parse_miles(row_get(raw_row, "Miles Traveled during day", "Miles Traveled During Day", "Miles"))
    on_h = parse_hm(row_get(raw_row, "Robot Time On During Day", "On Hours", "On Time", "OnHours"))
    motion_h = parse_hm(row_get(raw_row, "Robot Time In Motion During Day", "Motion Hours", "Motion Time", "MotionHours"))
    util_raw = row_get(raw_row, "Utilization")
    util = (motion_h / on_h) if on_h else parse_miles(util_raw)
    if util > 1.5:
        util = util / 100.0

    return {
        "Item": clean_text(row_get(raw_row, "Item")),
        "Date": dt.strftime("%Y-%m-%d"),
        "Location": clean_text(row_get(raw_row, "Location", "Customer")),
        "Robot": robot,
        "Version": clean_text(row_get(raw_row, "Version")),
        "Miles": float(miles),
        "OnHours": float(on_h),
        "MotionHours": float(motion_h),
        "Utilization": float(util or 0.0),
        "Source": clean_text(row_get(raw_row, "Source")) or default_source,
    }


def row_key(row: Dict[str, object]) -> Tuple[object, ...]:
    return (
        clean_text(row.get("Date", "")),
        clean_text(row.get("Robot", "")),
        clean_text(row.get("Location", "")),
        clean_text(row.get("Item", "")),
        round(float(row.get("Miles", 0.0) or 0.0), 4),
        round(float(row.get("OnHours", 0.0) or 0.0), 4),
        round(float(row.get("MotionHours", 0.0) or 0.0), 4),
    )


def dedupe_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    seen = set()
    out = []
    for row in rows:
        key = row_key(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    out.sort(key=lambda r: (clean_text(r.get("Date")), clean_text(r.get("Robot")), clean_text(r.get("Item"))))
    return out


def format_hours_hm(hours) -> str:
    total_minutes = int(round(float(hours or 0.0) * 60))
    hh = total_minutes // 60
    mm = total_minutes % 60
    return f"{hh}:{mm:02d}"


def fmt_float(value, digits: int = 6) -> str:
    text = f"{float(value or 0.0):.{digits}f}".rstrip("0").rstrip(".")
    return text or "0"


def read_csv_file(path: Path, default_source: str = "csv") -> List[Dict[str, object]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [normalize_row(row, default_source=default_source) for row in reader]
    return dedupe_rows([row for row in rows if row])


def write_dashboard_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DASHBOARD_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "Item": clean_text(row.get("Item")),
                "Date": clean_text(row.get("Date")),
                "Location": clean_text(row.get("Location")),
                "Robot": clean_text(row.get("Robot")),
                "Version": clean_text(row.get("Version")),
                "Miles": fmt_float(row.get("Miles"), 4),
                "OnHours": fmt_float(row.get("OnHours"), 6),
                "MotionHours": fmt_float(row.get("MotionHours"), 6),
                "Utilization": fmt_float(row.get("Utilization"), 6),
                "Source": clean_text(row.get("Source")),
            })


def write_canonical_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CANONICAL_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "Date": clean_text(row.get("Date")),
                "Location": clean_text(row.get("Location")),
                "Robot": clean_text(row.get("Robot")),
                "Item": clean_text(row.get("Item")),
                "Version": clean_text(row.get("Version")),
                "Robot Time On During Day": format_hours_hm(row.get("OnHours", 0.0)),
                "Robot Time In Motion During Day": format_hours_hm(row.get("MotionHours", 0.0)),
                "Miles Traveled during day": fmt_float(row.get("Miles"), 4),
                "Utilization": fmt_float(row.get("Utilization"), 6),
                "Source": clean_text(row.get("Source")),
            })


def compute_range(rows: List[Dict[str, object]]) -> Tuple[Optional[str], Optional[str]]:
    dates = sorted(clean_text(row.get("Date")) for row in rows if clean_text(row.get("Date")))
    return (dates[0], dates[-1]) if dates else (None, None)


def latest_dates(rows: List[Dict[str, object]], n: int = 12) -> List[str]:
    vals = sorted({clean_text(row.get("Date")) for row in rows if clean_text(row.get("Date"))})
    return vals[-n:]


def read_if_exists(path: Path, default_source: str) -> List[Dict[str, object]]:
    return read_csv_file(path, default_source=default_source) if path.exists() else []


def load_previous_state() -> Dict[str, object]:
    if not STATE_JSON.exists():
        return {}
    try:
        return json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: Dict[str, object]) -> None:
    STATE_JSON.write_text(json.dumps(state, indent=2, sort_keys=False), encoding="utf-8")


def file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_month_locks() -> Dict[str, object]:
    if MONTH_LOCKS_JSON.exists():
        try:
            return json.loads(MONTH_LOCKS_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"locked_months": {}}


def save_month_locks(locks: Dict[str, object]) -> None:
    MONTH_LOCKS_JSON.write_text(json.dumps(locks, indent=2, sort_keys=True), encoding="utf-8")


def write_monthly_archives_locked(rows: List[Dict[str, object]]) -> Dict[str, object]:
    locks = load_month_locks()
    locked_months = locks.setdefault("locked_months", {})
    current_month = now_local().strftime("%Y-%m")
    by_month: Dict[str, List[Dict[str, object]]] = {}
    for row in rows:
        d = clean_text(row.get("Date"))
        if d:
            by_month.setdefault(d[:7], []).append(row)

    late_arrivals: List[Dict[str, object]] = []

    for month_key, month_rows in sorted(by_month.items()):
        month_rows = dedupe_rows(month_rows)
        out_path = MONTHLY_ARCHIVE_DIR / f"SG_{month_key}.csv"
        is_closed_month = month_key < current_month

        if not is_closed_month:
            write_canonical_csv(out_path, month_rows)
            continue

        if not out_path.exists():
            write_canonical_csv(out_path, month_rows)
            locked_months[month_key] = {
                "locked_at": now_str(),
                "row_count_when_locked": len(month_rows),
                "filename": out_path.name,
            }
            continue

        existing_rows = read_csv_file(out_path, default_source=out_path.name)
        existing_keys = {row_key(r) for r in existing_rows}
        incoming_new = [r for r in month_rows if row_key(r) not in existing_keys]

        if month_key not in locked_months:
            locked_months[month_key] = {
                "locked_at": now_str(),
                "row_count_when_locked": len(existing_rows),
                "filename": out_path.name,
            }

        if incoming_new:
            late_arrivals.extend(incoming_new)

    if late_arrivals:
        existing_pending = read_if_exists(LATE_ARRIVALS_CSV, LATE_ARRIVALS_CSV.name)
        write_canonical_csv(LATE_ARRIVALS_CSV, dedupe_rows(existing_pending + late_arrivals))

    save_month_locks(locks)
    return {
        "current_open_month": current_month,
        "locked_month_count": len(locked_months),
        "late_arrivals_pending": len(read_if_exists(LATE_ARRIVALS_CSV, LATE_ARRIVALS_CSV.name)),
    }


def save_raw_snapshot(downloaded_csv_path: Path) -> Path:
    stamp = now_local().strftime("%Y%m%d_%H%M%S")
    target = RAW_SNAPSHOT_DIR / f"airtable_raw_{stamp}.csv"
    shutil.copyfile(downloaded_csv_path, target)
    return target


def wrong_page_reason(page) -> Optional[str]:
    try:
        html = page.content()
    except Exception:
        return None
    lower = html.lower()
    if "your browser version is not supported" in lower:
        return "Airtable served the unsupported-browser page instead of the shared view"
    if "low-code platform" in lower and "everyone&#39;s app platform" in lower:
        return "Airtable served the marketing/home page instead of the shared view"
    return None


def dismiss_popups(page) -> None:
    for label in ["Accept", "Accept all", "Got it", "Okay", "Close", "Dismiss"]:
        try:
            loc = page.get_by_text(label, exact=False)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=700)
                page.wait_for_timeout(150)
        except Exception:
            pass


def click_visible(locator) -> bool:
    count = min(locator.count(), 20)
    for i in range(count):
        item = locator.nth(i)
        try:
            if item.is_visible():
                item.click(timeout=1300)
                return True
        except Exception:
            pass
    return False


def try_direct_csv_url(page) -> Optional[bytes]:
    anchors = page.locator("a[href]")
    count = min(anchors.count(), 500)
    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href") or ""
            text = (anchors.nth(i).inner_text(timeout=500) or "").lower()
            if "csv" in href.lower() or "download csv" in text:
                response = page.request.get(href)
                if response.ok:
                    body = response.body()
                    if body and b"," in body[:1000]:
                        return body
        except Exception:
            pass
    return None


def open_menu_button(page) -> bool:
    selector_candidates = [
        'text="Download CSV"',
        'button:has-text("...")', 'button:has-text("…")', 'button:has-text("⋯")', 'button:has-text("•••")',
        '[role="button"]:has-text("...")', '[role="button"]:has-text("…")', '[role="button"]:has-text("⋯")', '[role="button"]:has-text("•••")',
        'button[aria-label*="more" i]', 'button[aria-label*="options" i]', 'button[aria-label*="menu" i]',
        '[role="button"][aria-label*="more" i]', '[role="button"][aria-label*="options" i]', '[role="button"][aria-label*="menu" i]',
        'button[aria-haspopup="menu"]', '[role="button"][aria-haspopup="menu"]',
    ]
    for selector in selector_candidates:
        try:
            if selector == 'text="Download CSV"':
                if page.locator(selector).count() > 0:
                    return True
            elif click_visible(page.locator(selector)):
                page.wait_for_timeout(250)
                return True
        except Exception:
            pass

    try:
        handles = page.locator("button, [role='button']")
        count = min(handles.count(), 80)
        for i in range(count):
            item = handles.nth(i)
            try:
                if not item.is_visible():
                    continue
                text = (item.inner_text(timeout=250) or "").strip()
                aria = (item.get_attribute("aria-label") or "").strip().lower()
                title = (item.get_attribute("title") or "").strip().lower()
                box = item.bounding_box() or {}
                y = box.get("y", 9999)
                if y > 220:
                    continue
                if text in ("...", "…", "⋯", "•••") or "more" in aria or "options" in aria or "menu" in aria or "more" in title or "options" in title:
                    item.click(timeout=1300)
                    page.wait_for_timeout(250)
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def click_download_csv_from_open_menu(page) -> bool:
    selectors = [
        'text="Download CSV"',
        'button:has-text("Download CSV")',
        '[role="menuitem"]:has-text("Download CSV")',
        '[role="button"]:has-text("Download CSV")',
        '[href*="downloadCsv"]',
        '[href*="download"]:has-text("CSV")',
    ]
    for selector in selectors:
        try:
            if click_visible(page.locator(selector)):
                return True
        except Exception:
            pass
    return False


def wait_for_shared_view(page) -> None:
    selectors = ['text="Download CSV"', 'text="Robot"', 'text="Date"', 'table']
    last_error = None
    for selector in selectors:
        try:
            page.wait_for_selector(selector, timeout=7000)
            return
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error


def harvest_airtable_csv(shared_url: str) -> Tuple[Path, str, float, Path]:
    temp_dir = Path(tempfile.mkdtemp(prefix="airtable_csv_"))
    target_path = temp_dir / "latest_airtable_download.csv"
    started = time.time()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--disable-background-networking"],
            )
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1440, "height": 960},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/Los_Angeles",
            )
            page = context.new_page()
            page.set_default_timeout(20000)
            page.goto(shared_url, wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                pass
            page.wait_for_timeout(1000)
            dismiss_popups(page)
            wait_for_shared_view(page)

            reason = wrong_page_reason(page)
            if reason:
                raise RuntimeError(reason)

            body = try_direct_csv_url(page)
            if body:
                target_path.write_bytes(body)
                browser.close()
                return target_path, "direct-link.csv", round(time.time() - started, 2), temp_dir

            try:
                with page.expect_download(timeout=25000) as download_info:
                    if click_download_csv_from_open_menu(page):
                        download = download_info.value
                        suggested = download.suggested_filename or "SG_Latest_Airtable.csv"
                        download.save_as(str(target_path))
                        browser.close()
                        return target_path, suggested, round(time.time() - started, 2), temp_dir
            except Exception:
                pass

            last_error = None
            for attempt in range(1, 7):
                try:
                    dismiss_popups(page)
                    if not open_menu_button(page):
                        last_error = RuntimeError("Could not find the three-dots menu button")
                    else:
                        with page.expect_download(timeout=25000) as download_info:
                            if not click_download_csv_from_open_menu(page):
                                raise RuntimeError("Three-dots menu opened, but Download CSV item was not found")
                            download = download_info.value
                            suggested = download.suggested_filename or "SG_Latest_Airtable.csv"
                            download.save_as(str(target_path))
                            browser.close()
                            return target_path, suggested, round(time.time() - started, 2), temp_dir
                except Exception as exc:
                    last_error = exc
                    try:
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(300)
                    except Exception:
                        pass
                    page.screenshot(path=str(DEBUG_DIR / f"airtable_attempt_{attempt}.png"), full_page=True)

            (DEBUG_DIR / "airtable_last_page.html").write_text(page.content(), encoding="utf-8", errors="ignore")
            browser.close()
            raise RuntimeError(f"Could not download CSV from Airtable shared view. Last error: {last_error}")
    except PlaywrightTimeoutError:
        raise RuntimeError("Timed out waiting for shared Airtable page CSV download")


def merge_history(seed_rows: List[Dict[str, object]], latest_rows: List[Dict[str, object]], existing_history_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    # Replace any dates present in the latest Airtable pull instead of appending them.
    # This prevents repeated runs during the same day from stacking cumulative metrics.
    refresh_dates = {clean_text(row.get("Date")) for row in latest_rows if clean_text(row.get("Date"))}
    preserved_seed = [row for row in seed_rows if clean_text(row.get("Date")) not in refresh_dates]
    preserved_history = [row for row in existing_history_rows if clean_text(row.get("Date")) not in refresh_dates]
    return dedupe_rows(list(preserved_seed) + list(preserved_history) + list(latest_rows))


def build_state(latest_rows: List[Dict[str, object]], merged_rows: List[Dict[str, object]], latest_download_name: str, latest_seconds: float, archive_info: Dict[str, object], changed: bool, shared_url: str) -> Dict[str, object]:
    range_start, range_end = compute_range(merged_rows)
    previous = load_previous_state()
    previous_version = int(previous.get("data_version") or 0)
    version_seed = previous_version + 1 if changed else previous_version

    return {
        "ok": True,
        "mode": "github online harvest + strict monthly locking",
        "last_success": now_str(),
        "last_attempt": now_str(),
        "last_error": None,
        "range_start": range_start,
        "range_end": range_end,
        "row_count": len(merged_rows),
        "changed": changed,
        "download_filename": latest_download_name,
        "download_seconds": latest_seconds,
        "latest_dates": latest_dates(merged_rows),
        "data_version": version_seed,
        "last_change_at": now_str() if changed else previous.get("last_change_at"),
        "next_even_refresh_at": None,
        "meta_poll_hint_seconds": 60,
        "history_filename": HISTORY_CSV.name,
        "history_rows": len(merged_rows),
        "merged_csv_filename": MERGED_CSV.name,
        "latest_csv_filename": LATEST_CSV.name,
        "state_filename": STATE_JSON.name,
        "current_open_month": archive_info.get("current_open_month"),
        "locked_month_count": archive_info.get("locked_month_count", 0),
        "late_arrivals_pending": archive_info.get("late_arrivals_pending", 0),
        "previous_data_version": previous.get("data_version"),
        "shared_page_url": shared_url,
        "source_csv_url": None,
        "live_source_name": "Airtable shared view",
        "normalized_date_format": "YYYY-MM-DD",
        "csv_parser": "Python csv module + robust normalized field aliases",
        "overlap_handling": "Latest pull replaces same-date rows before merge",
        "raw_snapshot_dir": RAW_SNAPSHOT_DIR.name,
        "monthly_archive_dir": MONTHLY_ARCHIVE_DIR.name,
        "sync_schedule": "Monday-Friday at 6:00 AM, 10:00 AM, and 2:00 PM Pacific",
    }


def save_failure_state(error: str, started_at: float) -> None:
    previous = load_previous_state()
    previous.update({
        "ok": False,
        "mode": "error",
        "last_attempt": now_str(),
        "last_error": error,
        "download_seconds": round(time.time() - started_at, 2),
        "meta_poll_hint_seconds": 60,
        "state_filename": STATE_JSON.name,
        "live_csv_filename": LATEST_CSV.name,
        "merged_csv_filename": MERGED_CSV.name,
        "history_filename": HISTORY_CSV.name,
        "live_source_name": "Airtable shared view",
        "sync_schedule": "Monday-Friday at 6:00 AM, 10:00 AM, and 2:00 PM Pacific",
    })
    save_state(previous)


def main() -> int:
    started_at = time.time()
    ensure_dirs()
    shared_url = clean_text(os.environ.get("AIRTABLE_SHARED_URL"))
    if not shared_url:
        save_failure_state("Missing AIRTABLE_SHARED_URL repository secret.", started_at)
        print("Missing AIRTABLE_SHARED_URL repository secret.", file=sys.stderr)
        return 1

    before_hash = file_hash(MERGED_CSV)
    temp_dir: Optional[Path] = None

    try:
        print("Harvesting Airtable shared page...")
        csv_path, suggested_filename, seconds, temp_dir = harvest_airtable_csv(shared_url)
        if not csv_path.exists():
            raise RuntimeError(f"Download completed but file was not saved: {csv_path}")

        latest_rows = read_csv_file(csv_path, default_source=suggested_filename)
        if not latest_rows:
            raise RuntimeError("Latest Airtable CSV downloaded but no usable rows were parsed.")

        snapshot_path = save_raw_snapshot(csv_path)
        shutil.copyfile(csv_path, LATEST_CSV)

        seed_rows = read_if_exists(HISTORY_CSV, HISTORY_CSV.name)
        existing_history_rows = read_if_exists(MASTER_HISTORY_CSV, MASTER_HISTORY_CSV.name)
        merged_rows = merge_history(seed_rows, latest_rows, existing_history_rows)

        write_canonical_csv(LATEST_CSV, latest_rows)
        write_canonical_csv(MASTER_HISTORY_CSV, merged_rows)
        write_dashboard_csv(MERGED_CSV, merged_rows)

        archive_info = write_monthly_archives_locked(merged_rows)

        after_hash = file_hash(MERGED_CSV)
        changed = before_hash != after_hash
        state = build_state(latest_rows, merged_rows, suggested_filename, seconds, archive_info, changed, shared_url)
        save_state(state)

        print("Done.")
        print(f"Latest rows           : {len(latest_rows)}")
        print(f"Merged rows           : {len(merged_rows)}")
        print("Overlap handling      : live pull replaces same-date rows before merge")
        print(f"Range                 : {state['range_start']} to {state['range_end']}")
        print(f"Current open month    : {state['current_open_month']}")
        print(f"Locked month count    : {state['locked_month_count']}")
        print(f"Late arrivals pending : {state['late_arrivals_pending']}")
        print(f"Snapshot              : {snapshot_path}")
        print(json.dumps(state, indent=2))
        return 0
    except Exception as exc:
        error = str(exc)
        save_failure_state(error, started_at)
        print(error, file=sys.stderr)
        return 1
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
