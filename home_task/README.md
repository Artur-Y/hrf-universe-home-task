# home_task module

This folder contains the main application code for the HRF Universe Home Task project.

## How to Run the CLI Script

The CLI script calculates and stores "days to hire" statistics in the database.

**Note:** The CLI is optimized for large datasets. All statistics calculations (percentiles, averages, outlier removal) are performed directly in the database using SQL, so the script does not load all job postings into memory and can efficiently handle millions of rows.

1. Make sure your database is up and migrations are applied.
2. Run the CLI script using Poetry:

```
poetry run python -m home_task.cli --min-postings 5
```

- `--min-postings` (optional): Minimum number of job postings required for statistics calculation (default: 5).

## How to Run the API Server

The API server provides endpoints to retrieve statistics.

Start the FastAPI server with:

```
poetry run uvicorn home_task.api:app --reload --port 8000
```

- The API will be available at http://localhost:8000
- Interactive docs: http://localhost:8000/docs

## Testing

You can run tests with:

```
poetry run pytest
```

## Configuration

Database connection settings are stored in `../secrets.ini`.

---

For more details, see the main project README.
