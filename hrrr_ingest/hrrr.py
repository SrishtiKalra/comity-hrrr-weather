from datetime import datetime, timedelta, timezone
from pathlib import Path
import io
import pandas as pd
import numpy as np
import s3fs, pygrib
from dateutil import parser as dateparser
from .variables import VARIABLES, ALL_SUPPORTED
from .geo import haversine_min_idx
from .log import log
import tempfile

BUCKET = "noaa-hrrr-bdp-pds"      # canonical bucket
DOMAIN = "conus"                  # per PDF scope

def _latest_complete_run_date(fs: s3fs.S3FileSystem) -> datetime.date:
    # Find latest YYYYMMDD/ that has f00..f48 for 06z. We’ll probe back a couple days.
    today = datetime.now(timezone.utc).date()
    for delta in range(0, 5):
        d = today - timedelta(days=delta)
        prefix = f"{BUCKET}/hrrr.{d:%Y%m%d}/{DOMAIN}/"
        # Check f48 existence
        key = f"{prefix}hrrr.t06z.wrfsfcf48.grib2"
        if fs.exists(key):
            return d
    raise RuntimeError("Could not find a recent complete 06z HRRR day in the last 5 days")

def s3_key(run_date, hour):
    return f"{BUCKET}/hrrr.{run_date:%Y%m%d}/{DOMAIN}/hrrr.t06z.wrfsfcf{hour:02d}.grib2"

def ingest(
    points: list[tuple[float, float]],
    run_date: str | None,
    variables: list[str] | None,
    num_hours: int,
) -> pd.DataFrame:
    fs = s3fs.S3FileSystem(anon=True)

    if run_date:
        rd = dateparser.parse(run_date).date()
    else:
        rd = _latest_complete_run_date(fs)

    vars_to_use = variables or ALL_SUPPORTED
    unknown = [v for v in vars_to_use if v not in VARIABLES]
    if unknown:
        raise ValueError(f"Unknown variables: {unknown}")

    rows = []
    for h in range(num_hours):
        key = s3_key(rd, h)
        if not fs.exists(key):
            log(f"[yellow]Missing S3 object[/yellow] s3://{key}, skipping hour {h}")
            continue

        # Read to memory (streaming)
        with fs.open(key, "rb") as f:
            data = f.read()

        with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
            tmp.write(data)
            tmp.flush()

            grbs = pygrib.open(tmp.name)
            try:
                # We’ll compute nearest index for the first message only (grid is shared)
                first = grbs.message(1)
                lats, lons = first.latlons()

                # Pre-compute nearest indices for each point
                nearest_idx = [haversine_min_idx((lats, lons), lat, lon) for (lat, lon) in points]

                # Build a lookup of variable → list of messages (robust match)
                needed = {k: VARIABLES[k] for k in vars_to_use}
                matched_msgs = {k: [] for k in vars_to_use}

                grbs.seek(0)

                def _in_levels(spec_levels, tol):
                    if spec_levels is None:
                        return True
                    if isinstance(spec_levels, (list, tuple, set)):
                        return tol in spec_levels
                    return tol == spec_levels

                for m in grbs:
                    sn  = (getattr(m, "shortName", "") or "").lower()
                    tol = getattr(m, "typeOfLevel", None)
                    lvl = getattr(m, "level", None)
                    desc = str(m)
                    desc_l = desc.lower()

                    for cli_name, spec in needed.items():
                        # primary: attribute match
                        sn_ok  = sn in {s.lower() for s in spec.short_names}
                        tol_ok = _in_levels(spec.type_of_level, tol)
                        lvl_ok = (spec.level is None) or (lvl == spec.level)

                        matched = sn_ok and tol_ok and lvl_ok

                        # fallback: case-insensitive substring hints (with tol/level gate)
                        if not matched and spec.contains_any:
                            tol_gate = _in_levels(spec.type_of_level, tol)
                            lvl_gate = (spec.level is None) or (lvl == spec.level)
                            if tol_gate and lvl_gate:
                                hints_l = tuple(h.lower() for h in spec.contains_any)
                                matched = any(h in desc_l for h in hints_l)

                        if matched:
                            matched_msgs[cli_name].append(m)

                # debug
                for cli_name in vars_to_use:
                    if not matched_msgs[cli_name]:
                        log(f"[yellow]No GRIB records matched for {cli_name} at hour {h}[/yellow]")



                run_time_utc = datetime(rd.year, rd.month, rd.day, 6, 0, 0, tzinfo=timezone.utc)
                for idx, (plat, plon) in enumerate(points):
                    ii, jj = nearest_idx[idx]
                    grid_lat = float(lats[ii, jj])
                    grid_lon = float(lons[ii, jj])

                    for cli_name in vars_to_use:
                        msgs = matched_msgs.get(cli_name, [])
                        if not msgs:
                            continue
                        m0 = msgs[0]
                        val = float(m0.values[ii, jj])
                        valid_time = m0.validDate.replace(tzinfo=timezone.utc)

                        rows.append({
                            "valid_time_utc": valid_time,
                            "run_time_utc": run_time_utc,
                            "latitude": grid_lat,
                            "longitude": grid_lon,
                            "variable": cli_name,
                            "value": val,
                            "source_s3": f"s3://{key}",
                        })
            finally:
                grbs.close()


    df = pd.DataFrame(rows, columns=[
        "valid_time_utc","run_time_utc","latitude","longitude","variable","value","source_s3"
    ])
    if not df.empty:
        # normalize grid coords to stable precision
        df["latitude"] = df["latitude"].round(6)
        df["longitude"] = df["longitude"].round(6)

        # drop duplicates within this batch
        df = df.drop_duplicates(
            subset=["valid_time_utc","run_time_utc","latitude","longitude","variable","source_s3"]
        )

    return df
