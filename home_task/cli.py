"""
CLI script for calculating days to hire statistics.

This script processes job posting data to calculate statistics per country and standard job:
- Removes outliers (lowest 10% and highest 10% of days_to_hire values)
- Calculates min (10th percentile), average, and max (90th percentile)
- Groups by country and standard job
- Saves results to days_to_hire_statistics table
"""

import argparse
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from home_task.db import get_session
from home_task.models import DaysToHireStatistics

# Configure logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- SQL Queries for Statistics Calculation ---

# This query calculates per-country statistics for each standard job.
# It uses window functions to assign deciles (10% buckets) to each row, then filters out the lowest and highest deciles (outliers).
# The 10th and 90th percentiles are calculated using percentile_cont, and the average is computed only for the filtered (middle 80%) values.
COUNTRY_STATS_QUERY = text(
    """
    WITH ranked AS (
        SELECT 
            standard_job_id,
            country_code,
            days_to_hire,
            COUNT(*) OVER (PARTITION BY standard_job_id, country_code) AS total_count,
            NTILE(10) OVER (PARTITION BY standard_job_id, country_code ORDER BY days_to_hire) AS decile_low,
            NTILE(10) OVER (PARTITION BY standard_job_id, country_code ORDER BY days_to_hire DESC) AS decile_high
        FROM job_posting
        WHERE days_to_hire IS NOT NULL
    ),
    filtered AS (
        SELECT * FROM ranked WHERE decile_low > 1 AND decile_high > 1
    )
    SELECT 
        r.standard_job_id,
        r.country_code,
        r.total_count,
        percentile_cont(0.1) WITHIN GROUP (ORDER BY r.days_to_hire) AS min_days,
        AVG(f.days_to_hire) AS avg_days,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY r.days_to_hire) AS max_days
    FROM ranked r
    LEFT JOIN filtered f ON r.standard_job_id = f.standard_job_id AND r.country_code = f.country_code AND r.days_to_hire = f.days_to_hire
    GROUP BY r.standard_job_id, r.country_code, r.total_count
    HAVING r.total_count >= :min_postings
    ORDER BY r.standard_job_id, r.country_code
    """
)

# This query calculates global statistics (all countries combined) for each standard job.
# The logic is the same as above, but without country_code grouping.
GLOBAL_STATS_QUERY = text(
    """
    WITH ranked AS (
        SELECT 
            standard_job_id,
            days_to_hire,
            COUNT(*) OVER (PARTITION BY standard_job_id) AS total_count,
            NTILE(10) OVER (PARTITION BY standard_job_id ORDER BY days_to_hire) AS decile_low,
            NTILE(10) OVER (PARTITION BY standard_job_id ORDER BY days_to_hire DESC) AS decile_high
        FROM job_posting
        WHERE days_to_hire IS NOT NULL
    ),
    filtered AS (
        SELECT * FROM ranked WHERE decile_low > 1 AND decile_high > 1
    )
    SELECT 
        r.standard_job_id,
        r.total_count,
        percentile_cont(0.1) WITHIN GROUP (ORDER BY r.days_to_hire) AS min_days,
        AVG(f.days_to_hire) AS avg_days,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY r.days_to_hire) AS max_days
    FROM ranked r
    LEFT JOIN filtered f ON r.standard_job_id = f.standard_job_id AND r.days_to_hire = f.days_to_hire
    GROUP BY r.standard_job_id, r.total_count
    HAVING r.total_count >= :min_postings
    ORDER BY r.standard_job_id
    """
)


def clear_existing_statistics(session: Session):
    """Delete all rows from the statistics table."""
    logger.info("Clearing existing statistics...")
    session.execute(text("DELETE FROM days_to_hire_statistics"))
    session.commit()


def calculate_country_statistics(session: Session, min_postings: int):
    """Calculate per-country statistics using SQL and return a list of DaysToHireStatistics objects."""
    logger.info(
        f"Fetching job posting data (minimum {min_postings} postings per group)..."
    )
    result = session.execute(COUNTRY_STATS_QUERY, {"min_postings": min_postings})
    statistics = []
    for row in result:
        stat_record = DaysToHireStatistics(
            id=None,
            standard_job_id=row.standard_job_id,
            country_code=row.country_code,
            min_days=row.min_days,
            avg_days=row.avg_days,
            max_days=row.max_days,
            job_postings_number=row.total_count,
        )
        statistics.append(stat_record)
    logger.info(f"Inserting {len(statistics)} statistics records...")
    return statistics


def calculate_global_statistics(session: Session, min_postings: int):
    """Calculate global statistics using SQL and return a list of DaysToHireStatistics objects."""
    logger.info("Calculating global statistics...")
    result = session.execute(GLOBAL_STATS_QUERY, {"min_postings": min_postings})
    statistics = []
    for row in result:
        stat_record = DaysToHireStatistics(
            id=None,
            standard_job_id=row.standard_job_id,
            country_code=None,
            min_days=row.min_days,
            avg_days=row.avg_days,
            max_days=row.max_days,
            job_postings_number=row.total_count,
        )
        statistics.append(stat_record)
    logger.info(f"Inserting {len(statistics)} global statistics records...")
    return statistics


def insert_statistics(session: Session, statistics: list):
    """Bulk insert statistics records (no commit)."""
    session.add_all(statistics)


def process_job_postings(session: Session, min_postings: int = 5) -> None:
    """
    Orchestrate the calculation and insertion of days-to-hire statistics.
    """
    logger.info("Starting statistics calculation...")
    clear_existing_statistics(session)
    country_stats = calculate_country_statistics(session, min_postings)
    insert_statistics(session, country_stats)
    global_stats = calculate_global_statistics(session, min_postings)
    insert_statistics(session, global_stats)
    total_records = len(country_stats) + len(global_stats)
    session.commit()
    logger.info(
        f"Statistics calculation completed! Inserted {total_records} records."
    )
    logger.info(f"- Country-specific statistics: {len(country_stats)}")
    logger.info(f"- Global statistics: {len(global_stats)}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Calculate days to hire statistics")
    parser.add_argument(
        "--min-postings",
        type=int,
        default=5,
        help="Minimum number of job postings required for statistics calculation (default: 5)",
    )

    args = parser.parse_args()

    try:
        with get_session() as session:
            process_job_postings(session, args.min_postings)
    except Exception as e:
        logger.error(f"Error calculating statistics: {e}")
        raise


if __name__ == "__main__":
    main()
