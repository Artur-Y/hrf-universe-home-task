"""
Tests for the CLI script that calculates and stores days-to-hire statistics.

Test coverage includes:
- Integration test: Ensures the CLI populates the statistics table.
- Skipping groups with fewer than the minimum postings.
- Correct calculation of statistics for a group with known data.
- Overwriting of existing statistics rows.
- Creation of global (world) statistics rows.

All tests use direct database manipulation for setup and teardown, and invoke the CLI as a subprocess to verify end-to-end behavior.
"""

import subprocess
import time

from sqlalchemy import text

from home_task.db import get_session


def test_cli_integration(tmp_path):
    """Integration test: run CLI and check DB for statistics rows."""
    # Run the CLI script as a subprocess
    result = subprocess.run(
        ["poetry", "run", "python", "-m", "home_task.cli", "--min-postings", "5"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"CLI failed: {result.stderr}"
    # Wait a moment for DB commit (should be instant, but just in case)
    time.sleep(1)
    # Check that statistics table has rows
    session = get_session()
    count = session.execute(
        text("SELECT COUNT(*) FROM days_to_hire_statistics")
    ).scalar()
    session.close()
    assert count > 0, "No statistics rows found after running CLI"


def insert_job_postings(session, postings):
    session.execute(text("DELETE FROM job_posting"))
    for p in postings:
        session.execute(
            text(
                """
                INSERT INTO job_posting (id, title, standard_job_id, country_code, days_to_hire)
                VALUES (:id, :title, :standard_job_id, :country_code, :days_to_hire)
            """
            ),
            p,
        )
    session.commit()


def fetch_statistics(session, standard_job_id, country_code=None):
    if country_code is None:
        result = session.execute(
            text(
                "SELECT * FROM days_to_hire_statistics WHERE standard_job_id=:sid AND country_code IS NULL"
            ),
            {"sid": standard_job_id},
        )
    else:
        result = session.execute(
            text(
                "SELECT * FROM days_to_hire_statistics WHERE standard_job_id=:sid AND country_code=:cc"
            ),
            {"sid": standard_job_id, "cc": country_code},
        )
    return result.fetchone()


def clear_statistics(session):
    session.execute(text("DELETE FROM days_to_hire_statistics"))
    session.commit()


def run_cli():
    result = subprocess.run(
        ["poetry", "run", "python", "-m", "home_task.cli", "--min-postings", "5"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"CLI failed: {result.stderr}"
    time.sleep(1)


def test_cli_skips_small_groups():
    session = get_session()
    clear_statistics(session)
    postings = [
        {
            "id": f"a{i}",
            "title": "t",
            "standard_job_id": "job1",
            "country_code": "US",
            "days_to_hire": 10 + i,
        }
        for i in range(4)
    ]
    insert_job_postings(session, postings)
    run_cli()
    stat = fetch_statistics(session, "job1", "US")
    session.close()
    assert (
        stat is None
    ), "Statistics should not be created for groups with < min_postings"


def test_cli_correct_statistics():
    session = get_session()
    clear_statistics(session)
    postings = [
        {
            "id": f"b{i}",
            "title": "t",
            "standard_job_id": "job2",
            "country_code": "DE",
            "days_to_hire": v,
        }
        for i, v in enumerate([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    ]
    insert_job_postings(session, postings)
    run_cli()
    stat = fetch_statistics(session, "job2", "DE")
    session.close()
    assert stat is not None
    # 10th percentile: 19, 90th percentile: 91, average of values between 19 and 91 (inclusive)
    assert abs(stat.min_days - 19) < 1
    assert abs(stat.max_days - 91) < 1
    assert 40 < stat.avg_days < 80


def test_cli_overwrites_existing_statistics():
    session = get_session()
    clear_statistics(session)
    # Insert statistics row manually
    session.execute(
        text(
            """
        INSERT INTO days_to_hire_statistics (standard_job_id, country_code, min_days, avg_days, max_days, job_postings_number)
        VALUES ('job3', 'FR', 1, 2, 3, 10)
    """
        )
    )
    session.commit()
    # Now insert postings and run CLI
    postings = [
        {
            "id": f"c{i}",
            "title": "t",
            "standard_job_id": "job3",
            "country_code": "FR",
            "days_to_hire": 10 + i,
        }
        for i in range(5)
    ]
    insert_job_postings(session, postings)
    run_cli()
    stat = fetch_statistics(session, "job3", "FR")
    session.close()
    assert stat is not None
    assert (
        stat.min_days != 1 and stat.max_days != 3
    ), "Statistics should be overwritten by CLI"


def test_cli_global_statistics():
    session = get_session()
    clear_statistics(session)
    postings = [
        {
            "id": f"d{i}",
            "title": "t",
            "standard_job_id": "job4",
            "country_code": cc,
            "days_to_hire": 10 + i,
        }
        for i, cc in enumerate(["US", "DE", "FR", None, "US"])
    ]
    # Add more to reach min_postings
    postings += [
        {
            "id": f"d{i+5}",
            "title": "t",
            "standard_job_id": "job4",
            "country_code": "US",
            "days_to_hire": 20 + i,
        }
        for i in range(5)
    ]
    insert_job_postings(session, postings)
    run_cli()
    stat = fetch_statistics(session, "job4", None)
    session.close()
    assert stat is not None, "Global statistics row should be created"
