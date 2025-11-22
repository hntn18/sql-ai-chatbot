"""
Simple utility functions for quick data ingestion to PostgreSQL
"""
import json
import pandas as pd
from sqlalchemy import create_engine
from config.config import settings
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_database_url():
    """Get PostgreSQL database URL"""
    return (
        f"postgresql://{settings.DATABASE_USERNAME}:"
        f"{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:"
        f"{settings.DATABASE_PORT}/{settings.DATABASE}"
    )


def ingest_json_to_postgres(json_file_path, table_name, if_exists='replace'):
    """
    Quick function to ingest JSON file to PostgreSQL using pandas
    
    Args:
        json_file_path: Path to JSON file
        table_name: Name of the target table
        if_exists: 'fail', 'replace', or 'append'
    """
    try:
        # Read JSON file
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Clean column names (remove spaces, lowercase)
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        
        logger.info(f"Loaded {len(df)} records from {json_file_path}")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Create database engine
        engine = create_engine(get_database_url())
        
        # Insert to database
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )
        
        logger.info(f"✓ Successfully ingested {len(df)} records into table '{table_name}'")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Error ingesting data: {e}")
        return False


def ingest_excel_to_postgres(excel_file_path, table_name, sheet_name=0, if_exists='replace'):
    """
    Quick function to ingest Excel file to PostgreSQL using pandas
    
    Args:
        excel_file_path: Path to Excel file
        table_name: Name of the target table
        sheet_name: Sheet name or index
        if_exists: 'fail', 'replace', or 'append'
    """
    try:
        # Read Excel file
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
        
        # Clean column names
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        
        logger.info(f"Loaded {len(df)} records from {excel_file_path}")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Create database engine
        engine = create_engine(get_database_url())
        
        # Insert to database
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )
        
        logger.info(f"✓ Successfully ingested {len(df)} records into table '{table_name}'")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Error ingesting data: {e}")
        return False


def ingest_csv_to_postgres(csv_file_path, table_name, if_exists='replace'):
    """
    Quick function to ingest CSV file to PostgreSQL using pandas
    
    Args:
        csv_file_path: Path to CSV file
        table_name: Name of the target table
        if_exists: 'fail', 'replace', or 'append'
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file_path)
        
        # Clean column names
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        
        logger.info(f"Loaded {len(df)} records from {csv_file_path}")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Create database engine
        engine = create_engine(get_database_url())
        
        # Insert to database
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )
        
        logger.info(f"✓ Successfully ingested {len(df)} records into table '{table_name}'")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Error ingesting data: {e}")
        return False


# Usage Examples
if __name__ == "__main__":
    # Ingest JSON file
    ingest_json_to_postgres('output.json', 'my_table', if_exists='replace')
    
    # Ingest Excel file
    # ingest_excel_to_postgres('data.xlsx', 'my_table', sheet_name=0)
    
    # Ingest CSV file
    # ingest_csv_to_postgres('data.csv', 'my_table')
