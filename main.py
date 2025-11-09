#!/usr/bin/env python3
"""
Main application entry point - Start the FastAPI server
"""
import uvicorn
from app.core.config import settings


def main():
    """Start the FastAPI server"""
    print("="*70)
    print(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    print("="*70)
    print(f"\nServer: http://{settings.API_HOST}:{settings.API_PORT}")
    print(f"API Docs: http://{settings.API_HOST}:{settings.API_PORT}/docs")
    print(f"ReDoc: http://{settings.API_HOST}:{settings.API_PORT}/redoc")
    print("\n" + "="*70 + "\n")

    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info"
    )


if __name__ == "__main__":
    main()
