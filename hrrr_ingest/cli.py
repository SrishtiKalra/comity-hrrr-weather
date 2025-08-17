# hrrr_ingest/cli.py
from __future__ import annotations

import typer
from typing import Optional, List
import pandas as pd
from .hrrr import ingest
from .db import connect, insert_df
from .variables import ALL_SUPPORTED
from .log import log

def main(
    points_file: str = typer.Argument(..., help="Path to points.txt (lat,lon per line)"),
    run_date: Optional[str] = typer.Option(None, "--run-date", help="YYYY-MM-DD (defaults to latest complete 06z)"),
    variables: Optional[str] = typer.Option(None, "--variables", help=f"Comma list (defaults to all: {','.join(ALL_SUPPORTED)})"),
    num_hours: int = typer.Option(48, "--num-hours", min=1, max=48, help="Forecast hours to ingest (default 48)"),
):
    # Read points (robust to BOM/blank lines/comments)
    pts: List[tuple[float, float]] = []
    with open(points_file, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.replace("\ufeff", "").strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) != 2:
                continue
            lat, lon = map(float, parts)
            pts.append((lat, lon))

    vars_list = variables.split(",") if variables else None

    log(f"Starting ingestion for {len(pts)} points | run_date={run_date or 'auto'} | hours={num_hours}")
    df: pd.DataFrame = ingest(pts, run_date, vars_list, num_hours)

    log(f"Parsed {len(df)} rows; writing to DuckDB...")
    con = connect("data.db")
    inserted = insert_df(con, df)
    con.close()
    log(f"[green]Done[/green]. Rows considered: {len(df)}; inserted (new): {inserted}")

# we keep the debug helper as a separate callable function;
def debug_list_vars(run_date: Optional[str] = typer.Option(None, "--run-date")):
    """
    Print unique (shortName, typeOfLevel, level) tuples and a few sample lines for hour 0.
    """
    from .hrrr import s3_key, _latest_complete_run_date
    import s3fs, tempfile, pygrib
    fs = s3fs.S3FileSystem(anon=True)
    rd = _latest_complete_run_date(fs) if not run_date else __import__("dateutil").parser.parse(run_date).date()
    key = s3_key(rd, 0)
    print(f"Inspecting s3://{key}")
    with fs.open(key, "rb") as f:
        data = f.read()
    with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
        tmp.write(data); tmp.flush()
        grbs = pygrib.open(tmp.name)
        try:
            seen = set()
            for m in grbs:
                tup = (getattr(m, "shortName", None), getattr(m, "typeOfLevel", None), getattr(m, "level", None))
                if tup not in seen:
                    seen.add(tup)
            print("Unique (shortName, typeOfLevel, level) values:")
            for t in sorted(seen, key=lambda x: (str(x[0]), str(x[1]), str(x[2]))):
                print(f"  {t}")
        finally:
            grbs.close()

def cli_main():
    import typer
    typer.run(main)

def cli_debug_vars():
    import typer
    typer.run(debug_list_vars)


# Uncomment the following lines to enable CLI execution

# if __name__ == "__main__":
#     typer.run(main)

