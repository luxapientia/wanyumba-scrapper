#!/usr/bin/env python3
"""
Database initialization script
Run this to create the database tables
"""
from sqlalchemy import text
from app.core.config import settings
from app.core.database import init_db, drop_db, engine
import sys
import os

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


def check_database_connection():
    """Test database connection"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful!")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print(f"\nDatabase URL: {settings.DATABASE_URL}")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running")
        print("  2. Database 'real_estate_db' exists")
        print("  3. Credentials are correct")
        print("\nTo create the database, run:")
        print("  psql -U postgres")
        print("  CREATE DATABASE real_estate_db;")
        return False


def main():
    """Main initialization function"""
    print("="*70)
    print("Real Estate Database Initialization")
    print("="*70)
    print(f"\nDatabase URL: {settings.DATABASE_URL}\n")

    # Check if user wants to drop existing tables
    if len(sys.argv) > 1 and sys.argv[1] == '--drop':
        response = input(
            "⚠️  WARNING: This will DROP all existing tables. Continue? (yes/no): ")
        if response.lower() == 'yes':
            print("\n🗑️  Dropping existing tables...")
            drop_db()
        else:
            print("Aborted.")
            return

    # Check database connection
    print("🔍 Checking database connection...")
    if not check_database_connection():
        return

    # Initialize database
    print("\n📊 Creating database tables...")
    try:
        init_db()
        print("\n" + "="*70)
        print("✅ Database initialized successfully!")
        print("="*70)
        print("\nTables created:")
        print("  - real_estate_listings")
        print("\nYou can now:")
        print("  - Run the API service: python main.py")
        print("  - Start scraping data")
        print("="*70)
    except Exception as e:
        print(f"\n❌ Error initializing database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
