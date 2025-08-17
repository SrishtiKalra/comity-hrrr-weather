import subprocess
import sys
import pytest

def test_imports():
    import hrrr_ingest.cli
    import hrrr_ingest.hrrr
    import hrrr_ingest.db
    import hrrr_ingest.variables

def test_cli_help():
    result = subprocess.run(
        ["hrrr-ingest", "--help"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    out = (result.stdout or "") + (result.stderr or "")
    assert "Usage:" in out or "Options:" in out
