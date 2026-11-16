# Can you beat Deutsche Bahn's own delay forecast?

*Draft. Numbers marked [final] come from `results/final.md` once the frozen model has been
scored on the fresh test days; the others are development numbers (12–16 November 2026).*

## The question

At a given moment, for an ICE, IC or EC train already running, how many minutes late will it
arrive at each of its next stops, 15 to 90 minutes ahead? The comparison that matters is DB's
own forecast: the Timetables API publishes a changed time for every coming stop, which is DB's
prediction at that moment. Scoring a model against it, at exactly the same moments, is the
test.

## Data

- **My own collector.** An async Python process polled the DB Timetables API for 40 major
  stations: all changes every 5 minutes, recent changes every minute at the 10 busiest
  stations, and the planned timetable hour by hour, about 19 requests a minute. Every response
  was kept (938 MB for five days), parsed into 220,000 change events, and joined into trips
  by the ride id and the trip's start time (the ride id alone repeats on other days).
- **The archive.** [piebro/deutsche-bahn-data](https://huggingface.co/datasets/piebro/deutsche-bahn-data)
  has a year of DB stops, with final times and, a few times a day per station, a snapshot of
  DB's change feed. I trained on July–October 2026.
- **"Actual" arrival.** The API has no actual time. I take DB's last published arrival
  time before the train got there, and only where my collector was polling; DB still moved it
  afterwards for 0.5 % of stops.

Two traps in the archive: the snapshot timestamps are UTC while every other time is Berlin
time (the snapshots stop changing exactly two hours "before" the arrival in summer, which
gave it away), and 3.5 % of the archive's "final" times were last seen before the train
arrived, so they are old forecasts, not arrivals. Both are handled; both would have quietly
skewed the training data.

## Model

One row is one train, one moment, one coming stop: "at 14:05, ICE 597 left Kassel 8 minutes
late; how late will it reach Frankfurt?". Features: the current delay and how it changed,
planned time left, time since the last departure, the stations, train type, hour, weekday,
this train's delay at this station over the previous 28 days, and, in the second model, DB's
own forecast. LightGBM with quantile objectives (10 / 50 / 90 %) gives the forecast and an
80 % range, calibrated with conformalized quantile regression on held-out weeks.

**The leak I caught.** My first training set took moments "15, 30, 60 or 90 minutes before
the actual arrival". With only four horizons, the planned time left gives the delay away, and
the model looked 53 % better than DB. Training moments now sit on a fixed clock grid while the
train is running, and a test checks that they and their features do not depend on the outcome.

## Results

[final] table from `results/final.md`. Development days, mean absolute error in minutes:

| Horizon | Persistence | DB | Model (with DB) | vs DB |
|---|---:|---:|---:|---:|
| 15 min | 4.91 | **2.10** | 2.23 | −6.1 % |
| 30 min | 5.69 | **3.11** | 3.13 | −0.6 % |
| 60 min | 7.67 | 5.13 | **4.80** | +6.4 % |
| 90 min | 9.37 | 7.13 | **6.25** | +12.3 % |

- **DB is hard to beat 15 minutes ahead.** Half of its errors are a minute or less.
- **At 60–90 minutes the model is closer**, mainly by correcting DB's optimism: trains
  arrive on average 6 minutes later than DB said 90 minutes before.
- **Without DB's forecast**, a model of delays at 40 stations beats "the delay stays" by
  11–26 % but not DB, which sees far more of the network.
- **The 80 % ranges cover 80–82 %** at every horizon.

## Where it fails

On-time trains: the model leans late (2.7 vs 1.9 min error). New incidents after the forecast
and sudden recoveries: neither DB nor the model sees them coming, and the worst 5 % of
forecasts make up a quarter of the error. The ranges are too narrow for big delays (76 %
coverage above 15 minutes late). And the gain is on busy weekdays; on a quiet weekend it is
close to zero.

## How it was checked

- Split by date, never randomly; the model was frozen (`frozen.json`, with SHA-256
  fingerprints, in git) before the final test days were collected, and scored on them once.
- The live site builds its features with the same code as training; replaying collected days
  through it gives the same scores as the offline evaluation (+8.4 vs +6.4 % at 60 minutes on
  a subset of the days, within noise).
- Anyone can repeat the final numbers from a 40 MB bundle: `dbdelay reproduce`.

## Limits

40 stations; the "actual" is DB's last word, not a measured arrival; the final test covers
[final] days rather than weeks, because the collector ran on a laptop; cancellations and
diversions are recorded but not forecast.

*Data: Deutsche Bahn (CC BY 4.0) via the DB API Marketplace; archive by piebro (data CC BY
4.0 by DB). Code: MIT.*
