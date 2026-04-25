# Sierra Gold Online Airtable Harvester

This project removes NetSuite and the local PC from the process.

It uses:

- GitHub Actions for the online scheduled harvest
- Playwright/Chromium for the Airtable shared-view CSV download
- GitHub repository files for online storage
- GitHub Pages for the dashboard website

## Schedule

The workflow is set to run Monday-Friday at:

- 6:00 AM Pacific
- 10:00 AM Pacific
- 2:00 PM Pacific

GitHub cron runs in UTC. The included workflow uses:

```yaml
- cron: "0 13,17,21 * * 1-5"
```

This matches Pacific daylight time. During Pacific standard time, change it to:

```yaml
- cron: "0 14,18,22 * * 1-5"
```

You can also run it manually from GitHub Actions with **Run workflow**.

## Files you will see in GitHub

- `data/SG_2024_Metrics.csv` - your seed/history CSV. Replace this with your real old history file.
- `data/SG_Latest_Airtable.csv` - latest Airtable pull.
- `data/SG_Merged.csv` - dashboard-ready merged data.
- `data/SG_History_Master.csv` - full canonical merged master.
- `data/SG_Dashboard_State.json` - status, row counts, date range, last error, version, monthly archive info.
- `data/raw_snapshots/` - raw Airtable CSV snapshots from each successful run.
- `data/monthly_archive/` - monthly archive CSV files, month locks, and late-arrival tracking.
- `data/debug/` - screenshots/HTML when Airtable CSV download fails.

## Rules carried over from the PowerShell monitor

- Online harvest uses a headless Chromium browser.
- It opens the vendor Airtable shared link.
- It dismisses common popups.
- It waits for a shared view marker such as Download CSV, Robot, Date, or table.
- It detects wrong Airtable pages, including unsupported-browser and marketing/home pages.
- It tries a direct CSV link first if one exists.
- It otherwise opens the three-dots/menu/options button.
- It clicks Download CSV from the open menu.
- It saves the latest pull.
- It saves raw snapshots.
- It normalizes dates to YYYY-MM-DD.
- It accepts multiple date formats.
- It accepts time fields as HH:MM, HH:MM:SS, AM/PM, numeric hours, and Excel time fractions.
- It normalizes field aliases such as Location/Customer, Miles/Miles Traveled during day, On Hours/Robot Time On During Day, Motion Hours/Robot Time In Motion During Day.
- It requires Date and Robot for a row to be usable.
- It computes Utilization as MotionHours divided by OnHours when OnHours exists.
- It dedupes by Date, Robot, Location, Item, Miles, OnHours, and MotionHours.
- It replaces same-date rows from the latest pull instead of appending them.
- It keeps previous history rows for dates not in the latest pull.
- It writes latest CSV, merged dashboard CSV, and canonical master history CSV.
- It creates monthly archive files.
- It locks closed months.
- It records late arrivals for locked months in `_late_arrivals_pending.csv`.
- It writes status/debug information to `SG_Dashboard_State.json`.

## Setup

1. Create a new GitHub repository.
2. Upload all files from this folder.
3. Replace `data/SG_2024_Metrics.csv` with your real history CSV.
4. In GitHub, go to **Settings > Secrets and variables > Actions > New repository secret**.
5. Add this secret:

```text
AIRTABLE_SHARED_URL
```

6. Paste your vendor Airtable shared link as the value.
7. Go to **Actions > Harvest Airtable CSV > Run workflow**.
8. After it finishes, check:

```text
data/SG_Merged.csv
data/SG_Dashboard_State.json
```

9. Go to **Settings > Pages** and set the source to **GitHub Actions**.
10. Your dashboard will be available at:

```text
https://YOUR-GITHUB-USERNAME.github.io/YOUR-REPO-NAME/
```

