import duckdb
from pathlib import Path



SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hrrr_forecasts (
  valid_time_utc TIMESTAMP,
  run_time_utc   TIMESTAMP,
  latitude       DOUBLE,
  longitude      DOUBLE,
  variable       VARCHAR,
  value          DOUBLE,
  source_s3      VARCHAR
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_hrrr
ON hrrr_forecasts(valid_time_utc, run_time_utc, latitude, longitude, variable, source_s3);
"""

KEY_COLS = "valid_time_utc, run_time_utc, latitude, longitude, variable, source_s3"

def connect(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute(SCHEMA_SQL)
    return con

def insert_df(con: duckdb.DuckDBPyConnection, df) -> int:
    if df is None or len(df) == 0:
        return 0

    # Register the DataFrame as a DuckDB view
    con.register("df", df)

    # Anti-join insert: only rows not present go in
    insert_sql = f"""
    INSERT INTO hrrr_forecasts
    SELECT df.*
    FROM df
    LEFT JOIN hrrr_forecasts t
    ON t.valid_time_utc = df.valid_time_utc
       AND t.run_time_utc = df.run_time_utc
       AND t.latitude = df.latitude
       AND t.longitude = df.longitude
       AND t.variable = df.variable
       AND t.source_s3 = df.source_s3
    WHERE t.valid_time_utc IS NULL
    """
    before = con.execute("SELECT COUNT(*) FROM hrrr_forecasts").fetchone()[0]
    con.execute(insert_sql)
    after = con.execute("SELECT COUNT(*) FROM hrrr_forecasts").fetchone()[0]
    return after - before


