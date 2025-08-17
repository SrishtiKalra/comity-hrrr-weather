# Comity HRRR Weather Ingest

CLI to ingest **NOAA High-Resolution Rapid Refresh (HRRR) 06z forecast runs** (up to 48 hours ahead) for specified lat/lon points & variables into **DuckDB** in **long format**.

---

## Setup
Clone the repo and set up your environment as follows:

```bash
# Create & activate virtualenv
python -m venv .venv && source .venv/bin/activate

# Install Python deps (editable mode for CLI entrypoint)
pip install -e .

# Install system deps (needed for pygrib)
# macOS
brew install eccodes jasper
# Ubuntu/Debian
sudo apt-get install libeccodes-dev libjasper-dev
```

## Supported Variables
The following variables are supported and mapped from HRRR GRIB2 records:

- `surface_pressure` – Surface pressure  
- `surface_roughness` – Surface roughness  
- `visible_beam_downward_solar_flux` – Visible beam downward solar flux  
- `visible_diffuse_downward_solar_flux` – Visible diffuse downward solar flux  
- `temperature_2m` – Temperature at 2m above ground  
- `dewpoint_2m` – Dew point temperature at 2m above ground  
- `relative_humidity_2m` – Relative humidity at 2m above ground  
- `u_component_wind_10m` – U wind component at 10m above ground  
- `v_component_wind_10m` – V wind component at 10m above ground  
- `u_component_wind_80m` – U wind component at 80m above ground  
- `v_component_wind_80m` – V wind component at 80m above ground  

## Document defaults

- --run-date defaults to latest complete 06z run in S3.
- --variables defaults to all supported variables.
- --num-hours defaults to 48.

## Example runs

# 2-hour smoke test (explicit date & vars):

```bash
hrrr-ingest points.txt --num-hours 2 --variables u_component_wind_80m,v_component_wind_80m
```

# 6-hour ingest (subset of vars):
```bash
hrrr-ingest points.txt --num-hours 6 --variables u_component_wind_80m,v_component_wind_80m,temperature_2m,surface_pressure
```

# Full 48-hour ingest (all variables by default):
```bash
hrrr-ingest points.txt
```


# Verifying the Data in DuckDB:
```bash
python - <<'PY'
import duckdb
con = duckdb.connect("data.db")

# 1) Total rows ingested
print(con.execute("select count(*) as total_rows from hrrr_forecasts").fetchdf())

# 2) Variable coverage (all 11 should appear)
print(con.execute("""
  select variable, count(*) as n
  from hrrr_forecasts
  group by 1
  order by 1
""").fetchdf())

# 3) Time window (should span the full 48h forecast horizon)
print(con.execute("""
  select min(valid_time_utc) as min_time, max(valid_time_utc) as max_time
  from hrrr_forecasts
""").fetchdf())

# 4) Per-point counts by variable (sanity check for nearest-grid sampling)
print(con.execute("""
  select variable, latitude, longitude, count(*) as n
  from hrrr_forecasts
  group by 1,2,3
  order by variable, n desc
""").fetchdf())
PY
```


# Notes
# Ingestion is idempotent (safe to re-run).
# surface_pressure may map to MSLP if surface PRES@surface isn’t present.
# The CLI can also be run with python -m hrrr_ingest.cli main ... for debugging, but the intended entrypoint is the installed script: 
```bash
hrrr-ingest points.txt
```

## Debugging & Variable Mapping

This project includes a helper command to inspect the raw GRIB2 files and list all unique (shortName, typeOfLevel, level) tuples present. This helps to:
- see warnings like No GRIB records matched for dewpoint_2m during ingestion.
- confirm which GRIB codes actually correspond to the human-readable names.
- extend the tool to support new HRRR variables.

```bash
hrrr-ingest debug-list-vars --run-date 2025-08-17
```

## Tests
A minimal smoke test is included to verify imports and -- help option
Run with:
```bash
pytest tests/




