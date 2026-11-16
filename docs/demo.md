# Demo video script (about 2 minutes)

Before recording: run `uv run dbdelay collect --hours 4 --keep-awake` for an hour or more
during the day, then `uv run dbdelay serve`. Without a live collection, use
`uv run dbdelay serve --replay-at 2026-11-13T17:00+01:00` (a busy Friday evening) and say on
screen that it is a replay.

1. **0:00–0:15. The question.** On the index page: "These are ICE, IC and EC trains running
   now. For each, DB publishes its own forecast of the next arrival. Can a model do better?"
2. **0:15–0:45. Search a train.** Type a train number from the list (a late one is best) and
   open it. Point at the table: planned time, DB's forecast in red, the model's in blue with
   its 80 % range.
3. **0:45–1:15. Watch it update.** Stay on the page for one or two refreshes (30 s). The
   chart below shows both forecasts for the next stop over time; say what changed.
4. **1:15–1:45. The scoreboard.** Open `/scoreboard`: error by horizon, the gain over DB at
   60 and 90 minutes, DB ahead at 15 minutes, ranges covering about 80 %.
5. **1:45–2:00. How you know it is real.** "Frozen before the test days, scored once, and
   anyone can repeat it from the data bundle with one command."
