from ingestion_scripts.ingest_receipts_data import create_stg_receipts_and_stg_receipts_items
from ingestion_scripts.ingest_users_data import create_stg_users
from ingestion_scripts.ingest_brands_data import create_stg_brands

from ingestion_scripts.ingestion_utils import configure_logger

logger = configure_logger("STG TABLES CREATION")

def create_stg_tables():
    """
    This function serves as the main entry point to create the staging tables and ingest
    data from the gzip files. 

    Ideally, in a production environment, this file and function would not exist, and 
    the ingestion scripts scripts would be orchestrated through a service like Apache 
    Airflow.
    """
    logger.info("Creating staging tables...")
    try:
        create_stg_receipts_and_stg_receipts_items()
        create_stg_users()
        create_stg_brands()
    except Exception as e:
        logger.info("Creation of staging tables failed")
        raise e