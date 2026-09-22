import shutil
import subprocess
import sys
from pathlib import Path


def test_etl_pipeline_runs_end_to_end_with_no_configuration(tmp_path):
    """
    Integration test for the full pipeline, not just the currency logic
    covered by test_currency.py.

    With no .env file present -- the exact situation a fresh clone of this
    repo is in -- the pipeline must fall back to a local SQLite database
    and complete successfully (extract, transform, validate, load) rather
    than crashing. This is the scenario the README advertises as the
    zero-configuration quick start, and it is what a stranger evaluating
    this project will actually try first.
    """
    repo_root = Path(__file__).resolve().parent
    for filename in ("etl_bolt_drive.py", "currency_parser.py"):
        shutil.copy(repo_root / filename, tmp_path / filename)

    result = subprocess.run(
        [sys.executable, "etl_bolt_drive.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )

    assert result.returncode == 0, (
        f"ETL pipeline exited with code {result.returncode}.\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert "PIPELINE RUN COMPLETED SUCCESSFULLY" in result.stdout
