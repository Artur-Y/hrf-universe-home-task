"""
Pytest-based tests for the FastAPI endpoints.
"""

import pytest
import requests
from sqlalchemy import text

from home_task.db import get_session

BASE_URL = "http://localhost:8000"


@pytest.fixture(scope="module")
def sample_data():
    """Fixture to provide sample job and country codes from the DB."""
    session = get_session()
    sample_query = text(
        """
        SELECT standard_job_id, country_code 
        FROM days_to_hire_statistics 
        WHERE country_code IS NOT NULL 
        LIMIT 1
    """
    )
    sample_result = session.execute(sample_query).fetchone()
    global_query = text(
        """
        SELECT standard_job_id 
        FROM days_to_hire_statistics 
        WHERE country_code IS NULL 
        LIMIT 1
    """
    )
    global_result = session.execute(global_query).fetchone()
    session.close()
    if not sample_result or not global_result:
        pytest.skip("No test data available. Please run the CLI script first.")
    return {
        "sample_job_id": sample_result.standard_job_id,
        "sample_country": sample_result.country_code,
        "global_job_id": global_result.standard_job_id,
    }


def test_health_check():
    """Test the /health endpoint returns a healthy status."""
    resp = requests.get(f"{BASE_URL}/health", timeout=3)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "service" in data


def test_country_specific_statistics(sample_data):
    """Test /statistics returns correct country-specific statistics for a job."""
    resp = requests.get(
        f"{BASE_URL}/statistics",
        params={
            "standard_job_id": sample_data["sample_job_id"],
            "country_code": sample_data["sample_country"],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["standard_job_id"] == sample_data["sample_job_id"]
    # Treat empty string and None as equivalent for country_code
    assert (data["country_code"] or "") == (sample_data["sample_country"] or "")
    assert isinstance(data["min_days"], float)
    assert isinstance(data["avg_days"], float)
    assert isinstance(data["max_days"], float)
    assert data["job_postings_number"] >= 5


def test_global_statistics(sample_data):
    """Test /statistics returns correct global statistics for a job."""
    resp = requests.get(
        f"{BASE_URL}/statistics",
        params={"standard_job_id": sample_data["global_job_id"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["standard_job_id"] == sample_data["global_job_id"]
    assert data["country_code"] is None
    assert isinstance(data["min_days"], float)
    assert isinstance(data["avg_days"], float)
    assert isinstance(data["max_days"], float)
    assert data["job_postings_number"] >= 5


def test_404_error():
    """Test /statistics returns 404 for a non-existent job."""
    resp = requests.get(
        f"{BASE_URL}/statistics", params={"standard_job_id": "non-existent-job"}
    )
    assert resp.status_code == 404
    data = resp.json()
    assert (
        "No global statistics found" in data["detail"]
        or "No statistics found" in data["detail"]
    )


def test_available_jobs():
    """Test /statistics/available-jobs returns a list of jobs with statistics."""
    resp = requests.get(f"{BASE_URL}/statistics/available-jobs")
    assert resp.status_code == 200
    data = resp.json()
    assert "available_jobs" in data
    assert "count" in data
    assert isinstance(data["available_jobs"], list)
    assert data["count"] == len(data["available_jobs"])


def test_available_countries(sample_data):
    """Test /statistics/available-countries returns countries for a given job."""
    resp = requests.get(
        f"{BASE_URL}/statistics/available-countries",
        params={"standard_job_id": sample_data["sample_job_id"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["standard_job_id"] == sample_data["sample_job_id"]
    assert "available_countries" in data
    assert "count" in data
    assert isinstance(data["available_countries"], list)
    assert data["count"] == len(data["available_countries"])
