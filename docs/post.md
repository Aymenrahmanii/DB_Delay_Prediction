# LinkedIn post (draft)

*Replace the [final] numbers from `results/final.md` before posting.*

---

Can a model beat Deutsche Bahn's own delay forecast?

For 8 weeks I built a forecaster for ICE, IC and EC trains and scored it against DB's own
prediction, published at the same moments, on days the model had never seen.

90 minutes before arrival, it is [final: 12 %] closer than DB's forecast. 15 minutes before,
DB wins: its forecast is hard to beat when the train is almost there.

What I learned:
→ DB's forecast is optimistic. Trains arrive on average 6 minutes later than DB says 90
minutes before. Correcting that is most of the gain.
→ My first model looked 53 % better than DB. It was a leak: the way I picked training
moments gave the answer away. A test now guards against it.
→ The archive I trained on stores one timestamp in UTC and every other in Berlin time.
Small details decide whether results are real.

Stack: Python, an async collector for the DB Timetables API, DuckDB and Parquet, LightGBM
quantile models with calibrated 80 % ranges, FastAPI with a page per train, and a scoreboard.

Code and a one-command way to repeat the numbers: [repo link]

Data: Deutsche Bahn, CC BY 4.0, via the DB API Marketplace.

[screenshot: scoreboard]
