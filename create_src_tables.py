import psycopg2

import constants
from ingestion_scripts.ingestion_utils import configure_logger
from ingestion_scripts import create_stg_tables

logger = configure_logger("SRC TABLES FILE")

def create_src_tables():
    """
    This function serves as the main entry point to create the source tables from the 
    staging tables that have been created. 

    Ideally, in a production environment, this file and function would not exist, and 
    the scripts would be orchestrated through a service like Apache Airflow or a tool like
    dbt.
    """
    conn = None
    cur = None
    try:
        # create staging tables
        create_stg_tables.create_stg_tables()

        logger.info("Creating source tables...")
        conn = psycopg2.connect(
            database = constants.POSTGRES_CREDENTIALS["db_name"], 
            user = constants.POSTGRES_CREDENTIALS["username"], 
            host = constants.POSTGRES_CREDENTIALS["hostname"],
            password = constants.POSTGRES_CREDENTIALS["password"],
            port = constants.POSTGRES_CREDENTIALS["port"]
        )
        cur = conn.cursor()
        table_list = ["users", "cpg", "brand_categories", "brands", "receipts", "items"]
        for table in table_list:
            cur.execute(open("{src_tables_base_path}src_{table_name}.sql".format(src_tables_base_path=constants.SRC_TABLES_BASE_PATH, table_name=table)).read())
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Successfully created source tables")
    except Exception as e:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.error(e, exc_info=True)
        raise e


if __name__ == "__main__":
    create_src_tables()
    