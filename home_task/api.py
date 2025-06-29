"""
FastAPI application for days to hire statistics API.

This module provides REST endpoints to retrieve calculated statistics
for job postings grouped by standard job and country.
"""

from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from home_task.db import get_session


# Pydantic models for API responses
class StatisticsResponse(BaseModel):
    """Response model for statistics data."""

    standard_job_id: str
    country_code: Optional[str]
    min_days: float
    avg_days: float
    max_days: float
    job_postings_number: int

    class Config:
        schema_extra = {
            "example": {
                "standard_job_id": "software-engineer",
                "country_code": "US",
                "min_days": 15.5,
                "avg_days": 28.7,
                "max_days": 45.2,
                "job_postings_number": 1250,
            }
        }


class ErrorResponse(BaseModel):
    """Response model for error messages."""

    detail: str


# Create FastAPI application
app = FastAPI(
    title="Days to Hire Statistics API",
    description="API for retrieving job posting statistics including days to hire metrics",
    version="1.0.0",
)


@app.get(
    "/statistics",
    response_model=StatisticsResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Statistics not found"},
        400: {"model": ErrorResponse, "description": "Invalid parameters"},
    },
    summary="Get days to hire statistics",
    description="""
    Retrieve days to hire statistics for a specific standard job.
    
    - **standard_job_id**: Required. The ID of the standard job to get statistics for.
    - **country_code**: Optional. If provided, returns country-specific statistics. 
                       If omitted, returns global statistics for the job.
    
    Returns min (10th percentile), average, and max (90th percentile) days to hire,
    along with the number of job postings used in the calculation.
    """,
)
async def get_statistics(
    standard_job_id: str = Query(
        ...,
        description="Standard job ID to get statistics for",
        example="software-engineer",
    ),
    country_code: Optional[str] = Query(
        None,
        description="Optional country code. If not provided, returns global statistics",
        example="US",
    ),
) -> StatisticsResponse:
    """
    Get days to hire statistics for a specific standard job and optional country.

    Args:
        standard_job_id: The standard job ID to get statistics for
        country_code: Optional country code. If None, returns global statistics

    Returns:
        StatisticsResponse: The calculated statistics

    Raises:
        HTTPException: 404 if no statistics found, 400 for invalid parameters
    """
    if not standard_job_id or not standard_job_id.strip():
        raise HTTPException(
            status_code=400,
            detail="standard_job_id parameter is required and cannot be empty",
        )

    try:
        with get_session() as session:
            # Build query based on whether country_code is provided
            if country_code:
                # Get country-specific statistics
                query = text(
                    """
                    SELECT 
                        standard_job_id,
                        country_code,
                        min_days,
                        avg_days,
                        max_days,
                        job_postings_number
                    FROM days_to_hire_statistics 
                    WHERE standard_job_id = :standard_job_id 
                    AND country_code = :country_code
                """
                )
                result = session.execute(
                    query,
                    {
                        "standard_job_id": standard_job_id,
                        "country_code": country_code,
                    },
                )
            else:
                # Get global statistics (country_code is NULL)
                query = text(
                    """
                    SELECT 
                        standard_job_id,
                        country_code,
                        min_days,
                        avg_days,
                        max_days,
                        job_postings_number
                    FROM days_to_hire_statistics 
                    WHERE standard_job_id = :standard_job_id 
                    AND country_code IS NULL
                """
                )
                result = session.execute(query, {"standard_job_id": standard_job_id})

            # Get the first (and should be only) result
            row = result.fetchone()

            if row is None:
                # No statistics found
                if country_code:
                    detail = f"No statistics found for standard_job_id '{standard_job_id}' and country_code '{country_code}'"
                else:
                    detail = f"No global statistics found for standard_job_id '{standard_job_id}'"

                raise HTTPException(status_code=404, detail=detail)

            # Return the statistics
            return StatisticsResponse(
                standard_job_id=row.standard_job_id,
                country_code=row.country_code,
                min_days=row.min_days,
                avg_days=row.avg_days,
                max_days=row.max_days,
                job_postings_number=row.job_postings_number,
            )

    except HTTPException:
        # Re-raise HTTP exceptions (404, 400, etc.)
        raise
    except Exception as e:
        # Handle any other unexpected errors
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error while retrieving statistics: {str(e)}",
        )


@app.get(
    "/health",
    summary="Health check endpoint",
    description="Simple health check to verify the API is running",
)
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "days-to-hire-statistics-api"}


# Add some additional useful endpoints
@app.get(
    "/statistics/available-jobs",
    summary="List available standard jobs",
    description="Get a list of all standard job IDs that have statistics available",
)
async def get_available_jobs():
    """Get list of all standard job IDs that have statistics."""
    try:
        with get_session() as session:
            query = text(
                """
                SELECT DISTINCT standard_job_id 
                FROM days_to_hire_statistics 
                ORDER BY standard_job_id
            """
            )
            result = session.execute(query)
            jobs = [row.standard_job_id for row in result]
            return {"available_jobs": jobs, "count": len(jobs)}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving available jobs: {str(e)}"
        )


@app.get(
    "/statistics/available-countries",
    summary="List available countries for a job",
    description="Get a list of all countries that have statistics for a specific standard job",
)
async def get_available_countries(
    standard_job_id: str = Query(
        ..., description="Standard job ID to get available countries for"
    )
):
    """Get list of countries that have statistics for a specific job."""
    try:
        with get_session() as session:
            query = text(
                """
                SELECT DISTINCT country_code 
                FROM days_to_hire_statistics 
                WHERE standard_job_id = :standard_job_id 
                AND country_code IS NOT NULL
                ORDER BY country_code
            """
            )
            result = session.execute(query, {"standard_job_id": standard_job_id})
            countries = [row.country_code for row in result]
            return {
                "standard_job_id": standard_job_id,
                "available_countries": countries,
                "count": len(countries),
            }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving available countries: {str(e)}"
        )
