"""
Migration script to update database schema from old to new structure

This script:
1. Adds new columns to the real_estate_listings table
2. Migrates data from old columns to new columns
3. Drops old columns that are no longer needed
"""
import logging
from app.core.database import engine, SessionLocal
from sqlalchemy import text
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def migrate_schema():
    """Migrate database schema from old to new structure"""

    with SessionLocal() as db:
        try:
            logger.info("Starting schema migration...")

            # Step 1: Rename 'source' column to 'target_site' if it exists
            logger.info("Step 1: Renaming 'source' to 'target_site'...")
            try:
                db.execute(text("""
                    ALTER TABLE real_estate_listings 
                    RENAME COLUMN source TO target_site
                """))
                db.commit()
                logger.info("✅ Renamed 'source' to 'target_site'")
            except Exception as e:
                if "does not exist" in str(e) or "already exists" in str(e):
                    logger.info("Column already migrated, skipping...")
                    db.rollback()
                else:
                    raise

            # Step 2: Add new columns
            logger.info("Step 2: Adding new columns...")
            new_columns = [
                ("currency", "VARCHAR(10)"),
                ("bedrooms", "INTEGER"),
                ("bathrooms", "INTEGER"),
                ("property_size", "FLOAT"),
                ("property_size_unit", "VARCHAR(20)"),
                ("property_type", "VARCHAR(50)"),
                ("listing_type", "VARCHAR(50)"),
                ("facilities", "TEXT[]"),
                ("contact_name", "VARCHAR(200)"),
                ("contact_phone", "TEXT[]"),
                ("contact_email", "TEXT[]"),
            ]

            for column_name, column_type in new_columns:
                try:
                    db.execute(text(f"""
                        ALTER TABLE real_estate_listings 
                        ADD COLUMN {column_name} {column_type}
                    """))
                    db.commit()
                    logger.info(f"✅ Added column '{column_name}'")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(
                            f"Column '{column_name}' already exists, skipping...")
                        db.rollback()
                    else:
                        logger.error(
                            f"Error adding column '{column_name}': {e}")
                        db.rollback()

            # Step 3: Change images column from JSON to TEXT[] if needed
            logger.info("Step 3: Updating images column type...")
            try:
                db.execute(text("""
                    ALTER TABLE real_estate_listings 
                    ALTER COLUMN images TYPE TEXT[] USING 
                    CASE 
                        WHEN images IS NULL THEN NULL
                        WHEN jsonb_typeof(images::jsonb) = 'array' THEN 
                            (SELECT array_agg(value::text) FROM jsonb_array_elements_text(images::jsonb))
                        ELSE ARRAY[]::TEXT[]
                    END
                """))
                db.commit()
                logger.info("✅ Updated images column to TEXT[]")
            except Exception as e:
                if "cannot be cast" in str(e) or "already" in str(e):
                    logger.info(
                        "Images column already correct type, skipping...")
                    db.rollback()
                else:
                    logger.error(f"Error updating images column: {e}")
                    db.rollback()

            # Step 4: Migrate data from old columns to new columns
            logger.info(
                "Step 4: Migrating data from seller_info to contact fields...")
            try:
                # Extract contact name from seller_info JSON
                db.execute(text("""
                    UPDATE real_estate_listings
                    SET contact_name = seller_info->>'name'
                    WHERE seller_info IS NOT NULL 
                    AND seller_info->>'name' IS NOT NULL
                    AND contact_name IS NULL
                """))

                # Extract phone numbers from seller_info JSON
                db.execute(text("""
                    UPDATE real_estate_listings
                    SET contact_phone = ARRAY(
                        SELECT jsonb_array_elements_text(seller_info->'phones')
                    )
                    WHERE seller_info IS NOT NULL 
                    AND seller_info->'phones' IS NOT NULL
                    AND contact_phone IS NULL
                """))

                db.commit()
                logger.info("✅ Migrated seller_info data to contact fields")
            except Exception as e:
                logger.warning(f"Warning during data migration: {e}")
                db.rollback()

            # Step 5: Extract property details from attributes
            logger.info(
                "Step 5: Extracting property details from attributes...")
            try:
                # Extract bedrooms
                db.execute(text("""
                    UPDATE real_estate_listings
                    SET bedrooms = CAST(
                        COALESCE(
                            attributes->>'Bedrooms',
                            attributes->>'bedrooms'
                        ) AS INTEGER
                    )
                    WHERE attributes IS NOT NULL 
                    AND bedrooms IS NULL
                    AND (attributes->>'Bedrooms' IS NOT NULL OR attributes->>'bedrooms' IS NOT NULL)
                """))

                # Extract bathrooms
                db.execute(text("""
                    UPDATE real_estate_listings
                    SET bathrooms = CAST(
                        COALESCE(
                            attributes->>'Bathrooms',
                            attributes->>'bathrooms'
                        ) AS INTEGER
                    )
                    WHERE attributes IS NOT NULL 
                    AND bathrooms IS NULL
                    AND (attributes->>'Bathrooms' IS NOT NULL OR attributes->>'bathrooms' IS NOT NULL)
                """))

                # Extract property type
                db.execute(text("""
                    UPDATE real_estate_listings
                    SET property_type = COALESCE(
                        attributes->>'Property Type',
                        attributes->>'propertyType'
                    )
                    WHERE attributes IS NOT NULL 
                    AND property_type IS NULL
                    AND (attributes->>'Property Type' IS NOT NULL OR attributes->>'propertyType' IS NOT NULL)
                """))

                db.commit()
                logger.info("✅ Extracted property details from attributes")
            except Exception as e:
                logger.warning(f"Warning during property extraction: {e}")
                db.rollback()

            # Step 6: Drop old columns that are no longer needed (optional)
            logger.info("Step 6: Dropping obsolete columns...")
            old_columns = ['views', 'posted_date', 'listing_id',
                           'image_count', 'scraped_at', 'seller_info']
            for column in old_columns:
                try:
                    db.execute(text(f"""
                        ALTER TABLE real_estate_listings 
                        DROP COLUMN IF EXISTS {column}
                    """))
                    db.commit()
                    logger.info(f"✅ Dropped column '{column}'")
                except Exception as e:
                    logger.warning(f"Could not drop column '{column}': {e}")
                    db.rollback()

            # Step 7: Add indexes for new columns
            logger.info("Step 7: Adding indexes for new columns...")
            indexes = [
                ("idx_bedrooms", "bedrooms"),
                ("idx_property_type", "property_type"),
                ("idx_listing_type", "listing_type"),
            ]

            for index_name, column_name in indexes:
                try:
                    db.execute(text(f"""
                        CREATE INDEX IF NOT EXISTS {index_name} 
                        ON real_estate_listings ({column_name})
                    """))
                    db.commit()
                    logger.info(f"✅ Created index '{index_name}'")
                except Exception as e:
                    logger.warning(
                        f"Could not create index '{index_name}': {e}")
                    db.rollback()

            logger.info("✅ Schema migration completed successfully!")

        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            db.rollback()
            raise


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Database Schema Migration")
    logger.info("=" * 60)

    response = input(
        "This will modify your database schema. Continue? (yes/no): ")
    if response.lower() != 'yes':
        logger.info("Migration cancelled.")
        sys.exit(0)

    migrate_schema()
    logger.info("=" * 60)
    logger.info("Migration complete!")
    logger.info("=" * 60)
