"""Draws docs/img/how-it-works.gif, the animated overview at the top of the README.

    uv run --with pillow python scripts/make_pipeline_gif.py

Pillow is only needed here, so it is not a project dependency. The numbers in the last frame
are the development results (README, "Results").
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

W, H = 960, 560
BG, FG, MUTED, LINE = "#fafaf8", "#1d1d1b", "#6b6b66", "#cfcdc6"
BLUE, BLUE_SOFT, RED = "#1f5fa8", "#e6eef9", "#c0392b"
FONTS = Path("C:/Windows/Fonts")
OUT = Path("docs/img/how-it-works.gif")


def font(name: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in (FONTS / name, Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")):
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


TITLE = font("segoeuib.ttf", 26)
BOX = font("seguisb.ttf", 19)
SMALL = font("segoeui.ttf", 15)
CAPTION = font("segoeui.ttf", 19)
STEP = font("seguisb.ttf", 15)

BW, BH, GAP, X0 = 190, 84, 40, 40
ROW = {1: 136, 2: 306}


def box_xy(col: int, row: int) -> tuple[int, int, int, int]:
    x = X0 + col * (BW + GAP)
    y = ROW[row]
    return x, y, x + BW, y + BH


# name, subtitle, column, row, caption
STAGES = [
    (
        "DB Timetables API",
        "plan · fchg · rchg",
        0,
        1,
        "DB publishes a changed time for every coming stop: its own forecast, updated all day.",
    ),
    (
        "Collector",
        "async · 40 stations",
        1,
        1,
        "An async collector polls 40 major stations: all changes every 5 min, the 10 busiest\n"
        "every minute (about 19 requests a minute).",
    ),
    (
        "Raw responses",
        "gzipped XML, all kept",
        2,
        1,
        "Every response is kept, so parsing can be fixed and re-run later\n(938 MB for five days).",
    ),
    (
        "Tables",
        "Parquet · DuckDB",
        3,
        1,
        "Parsed into stops and change events: DB's forecast at any moment is the latest\n"
        "change seen before it. Trips are joined across stations.",
    ),
    (
        "Archive",
        "Hugging Face · Jul–Oct",
        0,
        2,
        "A year of DB delays on Hugging Face trains the model, split by date:\n"
        "July to mid-October to learn, the rest of October to check.",
    ),
    (
        "Features as of t",
        "nothing after t",
        1,
        2,
        "At a moment t: the current delay, time left, stations, the train's recent history\n"
        "and DB's own forecast. Only data published before t.",
    ),
    (
        "Quantile models",
        "LightGBM 10 / 50 / 90 %",
        2,
        2,
        "LightGBM quantile models: the median is the forecast, 10-90 % the 80 % range,\n"
        "calibrated on held-out weeks.",
    ),
    (
        "Live site",
        "per train · scoreboard",
        3,
        2,
        "Every 30 s: a forecast for each coming stop, a page per train,\n"
        "and a nightly scoreboard against DB's forecast.",
    ),
]
# arrows: (from stage, to stage)
ARROWS = [(0, 1), (1, 2), (2, 3), (3, 5), (4, 5), (5, 6), (6, 7)]
INCOMING = {i: [a for a, b in ARROWS if b == i] for i in range(len(STAGES))}


def anchor(i: int, side: str) -> tuple[int, int]:
    x0, y0, x1, y1 = box_xy(STAGES[i][2], STAGES[i][3])
    return {
        "right": (x1, (y0 + y1) // 2),
        "left": (x0, (y0 + y1) // 2),
        "bottom": ((x0 + x1) // 2, y1),
        "top": ((x0 + x1) // 2, y0),
    }[side]


def ends(a: int, b: int) -> tuple[tuple[int, int], tuple[int, int]]:
    if STAGES[a][3] == STAGES[b][3]:
        return anchor(a, "right"), anchor(b, "left")
    return anchor(a, "bottom"), anchor(b, "top")


def arrow(d: ImageDraw.ImageDraw, p: tuple[int, int], q: tuple[int, int], color: str) -> None:
    d.line([p, q], fill=color, width=3)
    # arrow head
    import math

    ang = math.atan2(q[1] - p[1], q[0] - p[0])
    for side in (0.45, -0.45):
        d.line(
            [q, (q[0] - 12 * math.cos(ang + side), q[1] - 12 * math.sin(ang + side))],
            fill=color,
            width=3,
        )


def base(active: int, done: set[int]) -> Image.Image:
    im = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(im)
    d.text((X0, 30), "How it works", font=TITLE, fill=FG)
    d.text(
        (X0, 66),
        "Data from DB, a model that learns from a year of it, scored against DB live",
        font=SMALL,
        fill=MUTED,
    )
    for a, b in ARROWS:
        p, q = ends(a, b)
        arrow(d, p, q, BLUE if b in done or (b == active and a in done) else LINE)
    for i, (name, sub, col, row, _) in enumerate(STAGES):
        x0, y0, x1, y1 = box_xy(col, row)
        on, seen = i == active, i in done
        d.rounded_rectangle(
            (x0, y0, x1, y1),
            radius=12,
            fill=BLUE_SOFT if on else BG,
            outline=BLUE if on or seen else LINE,
            width=3 if on else 2,
        )
        d.text((x0 + 14, y0 + 16), name, font=BOX, fill=FG if on or seen else MUTED)
        d.text((x0 + 14, y0 + 46), sub, font=SMALL, fill=MUTED)
    # row labels
    d.text((X0, ROW[1] - 24), "COLLECT", font=STEP, fill=MUTED)
    d.text((X0, ROW[2] - 24), "LEARN AND PREDICT", font=STEP, fill=MUTED)
    return im


def caption(im: Image.Image, i: int) -> None:
    d = ImageDraw.Draw(im)
    y = 432
    d.line([(X0, y), (W - X0, y)], fill=LINE, width=1)
    d.text((X0, y + 18), f"{i + 1} / {len(STAGES)}", font=STEP, fill=BLUE)
    d.multiline_text((X0 + 60, y + 14), STAGES[i][4], font=CAPTION, fill=FG, spacing=8)


def frames() -> tuple[list[Image.Image], list[int]]:
    out: list[Image.Image] = []
    durations: list[int] = []
    done: set[int] = set()
    order = [0, 1, 2, 3, 4, 5, 6, 7]
    for i in order:
        moves = 7 if INCOMING[i] else 1
        for k in range(1, moves + 1):
            im = base(i, done)
            d = ImageDraw.Draw(im)
            for a in INCOMING[i]:
                if a not in done:
                    continue
                (px, py), (qx, qy) = ends(a, i)
                t = k / moves
                cx, cy = px + (qx - px) * t, py + (qy - py) * t
                d.ellipse((cx - 7, cy - 7, cx + 7, cy + 7), fill=BLUE)
            caption(im, i)
            out.append(im)
            durations.append(70)
        done.add(i)
        im = base(i, done)
        caption(im, i)
        out.append(im)
        durations.append(2600)
    out.append(results())
    durations.append(5000)
    return out, durations


def results() -> Image.Image:
    im = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(im)
    d.text((X0, 30), "Who forecasts better?", font=TITLE, fill=FG)
    d.text(
        (X0, 66),
        "Mean absolute error of the arrival delay in minutes, lower is better "
        "(development days, 12–16 Nov 2026)",
        font=SMALL,
        fill=MUTED,
    )
    data = [(15, 2.10, 2.23), (30, 3.11, 3.13), (60, 5.13, 4.80), (90, 7.13, 6.25)]
    top, bottom, scale = 104, 400, 32
    d.line([(X0, bottom), (W - X0, bottom)], fill=LINE, width=2)
    group = (W - 2 * X0) // len(data)
    for n, (h, db, model) in enumerate(data):
        gx = X0 + n * group + 40
        for k, (v, color) in enumerate(((db, RED), (model, BLUE))):
            x = gx + k * 70
            y = bottom - v * scale
            d.rectangle((x, y, x + 56, bottom), fill=color)
            d.text((x + 6, y - 24), f"{v:.2f}", font=STEP, fill=FG)
        gain = 1 - model / db
        d.text((gx, bottom + 12), f"{h} min ahead", font=BOX, fill=FG)
        d.text(
            (gx, bottom + 40),
            f"model {gain:+.0%} vs DB",
            font=SMALL,
            fill=BLUE if gain > 0 else MUTED,
        )
    d.rectangle((X0, top, X0 + 14, top + 14), fill=RED)
    d.text((X0 + 22, top - 4), "DB's own forecast", font=SMALL, fill=FG)
    d.rectangle((X0 + 180, top, X0 + 194, top + 14), fill=BLUE)
    d.text((X0 + 202, top - 4), "this model (it also sees DB's forecast)", font=SMALL, fill=FG)
    d.text(
        (X0, 500),
        "DB is hard to beat 15 minutes ahead. 60–90 minutes ahead the model is closer,"
        " mostly by correcting DB's optimism.",
        font=SMALL,
        fill=MUTED,
    )
    return im


def main() -> None:
    images, durations = frames()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    # One palette for every frame, from a strip of frames that holds all the colours.
    strip = Image.new("RGB", (W, H * 3))
    for k, im in enumerate((images[0], images[len(images) // 2], images[-1])):
        strip.paste(im, (0, H * k))
    palette = strip.quantize(colors=96)
    paletted = [im.quantize(palette=palette, dither=Image.Dither.NONE) for im in images]
    paletted[0].save(
        OUT,
        save_all=True,
        append_images=paletted[1:],
        duration=durations,
        loop=0,
        optimize=True,
        disposal=1,
    )
    print(f"wrote {OUT} ({OUT.stat().st_size / 1e6:.2f} MB, {len(images)} frames)")


if __name__ == "__main__":
    main()
