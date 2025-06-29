#!/usr/bin/env python3
"""
Server startup script for the Days to Hire Statistics API.
"""

import uvicorn
from home_task.api import app


def start_server():
    """Start the FastAPI server."""
    print("Starting Days to Hire Statistics API server...")
    print("API Documentation will be available at: http://localhost:8000/docs")
    print("API itself will be available at: http://localhost:8000")
    print("\nPress Ctrl+C to stop the server")
    
    uvicorn.run(
        "home_task.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    start_server()