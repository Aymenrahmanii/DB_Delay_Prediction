"""The live site: a page per train with DB's forecast and the model's, search, and the
scoreboard. Run with ``dbdelay serve``: a background task runs ``Live.cycle`` every
``REFRESH_S`` seconds; pages read the latest forecasts and poll ``/api/train/<trip>``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from dbdelay.ops import maintain
from dbdelay.score import load_scores, totals
from dbdelay.site import MAIN_MODEL, REFRESH_S, Live

log = logging.getLogger(__name__)

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def create_app(live: Live, run_loop: bool = True) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = None
        if run_loop:

            async def loop() -> None:
                while True:
                    try:
                        await asyncio.to_thread(live.cycle)
                        await asyncio.to_thread(maintain, live)
                    except Exception:  # keep serving the last forecasts
                        log.exception("live cycle failed")
                    await asyncio.sleep(REFRESH_S)

            task = asyncio.create_task(loop())
        yield
        if task:
            task.cancel()

    app = FastAPI(title="DB delay forecasts", lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(
            request, "index.html", {"trains": live.running(), "status": live.status()}
        )

    @app.get("/search", response_model=None)
    def search(request: Request, q: str = "") -> HTMLResponse | RedirectResponse:
        hits = live.search(q)
        if len(hits) == 1:
            return RedirectResponse(f"/train/{hits[0]}", status_code=303)
        trains = [t for t in (live.train(h) for h in hits) if t]
        return TEMPLATES.TemplateResponse(
            request, "search.html", {"q": q, "trains": trains, "status": live.status()}
        )

    @app.get("/train/{trip_key}", response_class=HTMLResponse)
    def train(request: Request, trip_key: str) -> HTMLResponse:
        data = live.train(trip_key)
        if data is None:
            raise HTTPException(404, "no such trip in the last hours")
        return TEMPLATES.TemplateResponse(request, "train.html", {"t": data})

    @app.get("/api/train/{trip_key}")
    def api_train(trip_key: str) -> JSONResponse:
        data = live.train(trip_key)
        if data is None:
            raise HTTPException(404, "no such trip in the last hours")
        return JSONResponse(data)

    def board(source: str) -> dict[str, Any]:
        settings = live.settings
        days = load_scores(settings.data_dir / "scores", source) if settings else []
        per_day = [
            {
                "day": s.day,
                "forecasts": s.forecasts,
                "rows": [r for r in s.rows if r["model"] == MAIN_MODEL],
            }
            for s in reversed(days)
        ]
        return {
            "source": source,
            "days": per_day,
            "totals": totals(days, MAIN_MODEL),
            "totals_without_db": totals(days, "without_db"),
        }

    @app.get("/scoreboard", response_class=HTMLResponse)
    def scoreboard(request: Request, source: str = "live") -> HTMLResponse:
        if source not in ("live", "replay"):
            raise HTTPException(404, "source is live or replay")
        return TEMPLATES.TemplateResponse(request, "scoreboard.html", {"b": board(source)})

    @app.get("/api/scoreboard")
    def api_scoreboard(source: str = "live") -> dict[str, Any]:
        return board(source)

    @app.get("/api/status")
    def api_status() -> dict[str, Any]:
        return live.status()

    return app
