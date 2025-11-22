"""
Script to ingest output.json into PostgreSQL database
"""
import json
import pandas as pd
from sqlalchemy import create_engine, text
from config.config import settings
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_database_url():
    """Get PostgreSQL database URL"""
    return (
        f"postgresql://{settings.DATABASE_USERNAME}:"
        f"{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:"
        f"{settings.DATABASE_PORT}/{settings.DATABASE}"
    )


def ingest_output_json():
    """
    Ingest output.json file to PostgreSQL database
    The file has a nested structure with 'Application_Applicant' key
    """
    try:
        # Read JSON file
        logger.info("Reading output.json file...")
        with open('output.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if data is nested (has 'Application_Applicant' key)
        if isinstance(data, dict) and 'Application_Applicant' in data:
            logger.info("Found nested structure with 'Application_Applicant' key")
            data = data['Application_Applicant']
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Clean column names (lowercase, replace spaces with underscores)
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        
        logger.info(f"✓ Loaded {len(df)} records")
        logger.info(f"✓ Columns ({len(df.columns)}): {list(df.columns)[:10]}...")  # Show first 10 columns
        logger.info(f"✓ DataFrame shape: {df.shape}")
        
        # Create database engine
        logger.info("Connecting to PostgreSQL database...")
        engine = create_engine(get_database_url())
        
        # Define table name
        table_name = 'application_applicant'
        
        # Insert to database
        logger.info(f"Ingesting data into table '{table_name}'...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # Options: 'fail', 'replace', 'append'
            index=False,
            chunksize=1000,
            method='multi'
        )
        
        logger.info(f"✓ Successfully ingested {len(df)} records into table '{table_name}'")
        logger.info(f"✓ Table columns: {list(df.columns)}")
        
        # Verify ingestion
        logger.info("Verifying data ingestion...")
        verify_query = text(f"SELECT COUNT(*) FROM {table_name}")
        with engine.connect() as conn:
            result = conn.execute(verify_query)
            count = result.scalar()
            logger.info(f"✓ Verified: {count} records in database")
        
        return True
        
    except FileNotFoundError:
        logger.error("✗ Error: output.json file not found!")
        logger.error("Make sure the file exists in the current directory")
        return False
        
    except Exception as e:
        logger.error(f"✗ Error ingesting data: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Starting data ingestion process...")
    print("=" * 60)
    
    success = ingest_output_json()
    
    print("=" * 60)
    if success:
        print("✓ Data ingestion completed successfully!")
    else:
        print("✗ Data ingestion failed. Check logs above.")
    print("=" * 60)
