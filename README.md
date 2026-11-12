# DB Delay Prediction

Predict how late a Deutsche Bahn long-distance train (ICE, IC, EC) will be at its next stops,
15–90 minutes ahead, and score it against DB's own forecast published at the same moment.

> Status: **week 1 — collector.** Results table goes here in week 8.

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
