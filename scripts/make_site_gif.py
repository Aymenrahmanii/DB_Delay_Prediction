"""Turns screenshots of the site into docs/img/site.gif, with a caption under each.

    uv run --with pillow python scripts/make_site_gif.py index.png search.png train.png \
        table.png scoreboard.png

The screenshots in the README were taken from ``dbdelay serve --replay-at
2026-11-15T15:00+01:00`` at 800x520 in light mode; the captions below follow that order.
"""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

W, H, BAR = 800, 520, 52
BG, FG, MUTED, BLUE = "#fafaf8", "#1d1d1b", "#6b6b66", "#1f5fa8"
OUT = Path("docs/img/site.gif")
CAPTIONS = [
    "Trains running now: the current delay, DB's forecast and the model's for the next stop",
    "Search a train by its number",
    "Delay by station: DB's forecast (red), the model's with its 80 % range (blue)",
    "Every coming stop, redrawn every 30 seconds as new data arrives",
    "The scoreboard: model, DB and persistence scored on the same forecasts",
]


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    name = "seguisb.ttf" if bold else "segoeui.ttf"
    path = Path("C:/Windows/Fonts") / name
    return ImageFont.truetype(str(path), size) if path.exists() else ImageFont.load_default()


def frame(shot: Path, n: int, total: int) -> Image.Image:
    im = Image.new("RGB", (W, H + BAR), BG)
    src = Image.open(shot).convert("RGB")
    im.paste(src.crop((0, 0, min(src.width, W), min(src.height, H))), (0, 0))
    d = ImageDraw.Draw(im)
    d.line([(0, H), (W, H)], fill="#cfcdc6", width=1)
    d.text((16, H + 15), f"{n} / {total}", font=font(15, bold=True), fill=BLUE)
    d.text((70, H + 13), CAPTIONS[n - 1], font=font(17), fill=FG)
    d.text((W - 150, H + 34), "replay of 15 Nov 2026", font=font(11), fill=MUTED)
    return im


def main(shots: list[Path]) -> None:
    if len(shots) != len(CAPTIONS):
        sys.exit(f"give {len(CAPTIONS)} screenshots, in the caption order")
    stills = [frame(s, k + 1, len(shots)) for k, s in enumerate(shots)]
    images: list[Image.Image] = []
    durations: list[int] = []
    for k, still in enumerate(stills):
        images.append(still)
        durations.append(3200)
        nxt = stills[(k + 1) % len(stills)]
        for t in (0.5,):  # a short cross-fade to the next screen
            images.append(Image.blend(still, nxt, t))
            durations.append(120)
    # Each frame its own palette: one shared palette loses DB's red in the screenshots.
    paletted = [im.convert("P", palette=Image.Palette.ADAPTIVE, colors=256) for im in images]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    paletted[0].save(
        OUT,
        save_all=True,
        append_images=paletted[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"wrote {OUT} ({OUT.stat().st_size / 1e6:.2f} MB, {len(images)} frames)")


if __name__ == "__main__":
    main([Path(a) for a in sys.argv[1:]])
