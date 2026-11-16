# DB Delay Prediction

Predict how late a Deutsche Bahn long-distance train (ICE, IC, EC) will be at its next stops,
15–90 minutes ahead, and score it against DB's own forecast published at the same moment.

> Status: **week 3: ground truth and baselines.** Results table goes here in week 8.

## Week 1: the collector

An async process polls the DB Timetables API for 40 major long-distance stations:

| Endpoint | What | Schedule | Requests/min |
|---|---|---|---|
| `/fchg/{eva}` | all of today's changes | every 5 min, 40 stations | 8 |
| `/rchg/{eva}` | changes of the last ~2 min | every 60 s, 10 busiest | 10 |
| `/plan/{eva}/{date}/{hour}` | planned stops of one hour | current hour + 3 ahead, once each | ~0.7 |

About 19 requests a minute, a third of the 60/min reported for the free plan. A shared token
bucket (`DBDELAY_RATE_LIMIT_PER_MINUTE`, default 50) caps the whole process; 429/5xx and network
errors are retried with backoff.

Every response is kept, so parsing bugs can be fixed and re-run later:

```
data/raw/<endpoint>/<YYYY-MM-DD>/<HH>/<eva>.jsonl.gz   # one gzip member per response
data/polls/<YYYY-MM-DD>.jsonl                          # one line per scheduled poll
```

fchg/rchg are partitioned by fetch time (UTC), plan by the requested slot (Berlin time).

### Run it locally

```bash
uv sync
cp .env.example .env   # add DBDELAY_DB_CLIENT_ID and DBDELAY_DB_API_KEY
uv run dbdelay stations-verify   # checks every EVA number against /station
uv run dbdelay collect
uv run dbdelay health --hours 24
```

### A fixed-length session on a laptop

```powershell
uv run dbdelay --log-file data/logs/collector.log collect --hours 48 --keep-awake
```

`--hours` stops the collector on its own; `--keep-awake` blocks sleep only while the process is
alive (Windows `SetThreadExecutionState`), so normal sleep returns when it exits, crashes or is
stopped with Ctrl+C. No power settings are changed. The screen may still turn off; closing the lid
still sleeps the machine.

(`deploy\windows\install-tasks.ps1` instead registers an always-on collector as a scheduled task.)

### Deploy (Debian/Ubuntu VM)

```bash
sudo ./deploy/install.sh   # first run creates /opt/dbdelay/.env; fill it in and run again
```

This installs `dbdelay-collector.service` (always on, restarts forever), an hourly health check
that alerts on Telegram when uptime drops below 99 %, and a daily 08:00 report.

Health counts polls that were *due* from the schedule, so time the process was down counts
against uptime, and lists every gap longer than two intervals per station.

## Week 2: parse and join

```bash
uv sync --group analysis
uv run dbdelay build-tables                          # raw responses -> data/tables/*.parquet
uv run dbdelay trip "ICE 597" --date 2026-11-15      # one trip, stop by stop
uv run dbdelay trip "ICE 597" --date 2026-10-15 --source archive
uv run dbdelay check --date 2026-11-15               # data checks for one day
```

`build-tables` keeps long-distance trains only and writes three tables:

| Table | One row per |
|---|---|
| `plan_stops.parquet` | planned stop (train at a station): times, platforms, line, origin, destination |
| `change_events/<day>.parquet` | time DB's view of a stop *changed*, stamped with the fetch that first saw it |
| `stop_observations.parquet` | stop: first and last fetch that mentioned it, number of fetches |

Fetches that repeat the previous state are dropped, which also removes the duplicates from two
collectors running at once. DB's forecast at any moment is the latest change event at or before
it, which week 3 uses.

`dbdelay.stops` reads both sources into one stop table with the same columns (planned and
changed times, delays, cancelled, added, platform changed). A trip is the ride id plus the
trip's start time from the stop id: the ride id alone recurs on other days. The collector only
sees its 40 stations, so its trips have gaps between them; the archive has every station.

Data facts the tables handle:

- **Diversions.** DB numbers the stops of a rerouted trip from 100 up, so they can repeat a
  station and run out of order; they are flagged `is_diversion`, and trips are ordered by
  planned time.
- **Extra trains** (`tl t="e"`) and stops with status `a` are `is_added`. A train whose
  `/plan` hour was never fetched (collector down) is kept with `in_plan = false`, not as added.
- **Forecast revisions.** DB often moves a forecast by a minute and back; those are real
  events, not parsing noise (98 % happen within one feed).

`dbdelay check` runs the daily checks: duplicate stops or events, missing keys, stations
outside the list, stop order, implausible delays, stops never seen, and poll coverage. Errors
exit with code 1; warnings are data facts.

Sanity check: arrival delays at the 40 stations, 10/25/50/75/90th percentile, in minutes:
archive October 2026 `0 / 1 / 5 / 18 / 37`, collector 12–16 November `0 / 1 / 6 / 19 / 40`.

## Week 3: ground truth and baselines

```bash
uv run dbdelay baselines             # horizon back from the actual arrival (default)
uv run dbdelay baselines --basis planned --by-hour
```

- **Actual arrival**: DB's last published arrival time among the changes seen no later than
  5 min after the time they announced, kept only if the collector polled that station in the
  10 min before. DB still moved the time afterwards for 0.5 % of stops (a limit of the method).
- **Snapshots**: for each target stop and horizon h, the moment t = arrival − h. Every value is
  as of t: DB's forecast is the latest change seen by t (planned if none), persistence is the
  train's delay at the last collected stop it had already left. No poll of the target station
  in the 10 min before t, no snapshot. Written to `data/tables/snapshots.parquet`.
- All three baselines are scored on the same rows: trains already running at t.

Mean absolute error of the arrival delay, minutes, 12–16 November 2026, 40 stations:

| Horizon | Snapshots | Timetable | Persistence | DB's forecast |
|---|---:|---:|---:|---:|
| 15 min | 10,177 | 14.8 | 4.9 | **2.1** |
| 30 min | 9,644 | 15.1 | 5.7 | **3.1** |
| 60 min | 8,362 | 15.9 | 7.7 | **5.1** |
| 90 min | 7,006 | 17.1 | 9.4 | **7.1** |

DB's forecast is optimistic: trains arrive on average 1.1 min later than DB said 15 minutes
before, and 6.0 min later 90 minutes before. Measured back from the *planned* arrival instead,
DB's error is 4.0 / 5.2 / 7.2 / 8.9 min.

### The archive

[piebro/deutsche-bahn-data](https://huggingface.co/datasets/piebro/deutsche-bahn-data) is used
for training history. To see its columns without downloading it:

```bash
uv sync --group analysis
uv run dbdelay archive-schema --month 2026-10 --sample 3
```

## Development

```bash
uv sync --all-groups
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
```

## Data and licence

Timetable data © Deutsche Bahn, CC BY 4.0, via the DB API Marketplace. Archive by piebro (data
CC BY 4.0 by DB, code MIT). Code in this repository: MIT.
