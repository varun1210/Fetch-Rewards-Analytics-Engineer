import logging
import psycopg2
import sqlalchemy
import pandas as pd
import numpy as np

import constants

MODULE_NAME = "INGESTION UTILS"


def configure_logger(module_name):
    """
    This function configures the loggers used across the application. 
    
    Args:
        module_name: Module for which logger is initialized

    Returns:
        Logger for the module
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler("ingestion_logs.txt")
    formatter = logging.Formatter('%(levelname)s:%(name)s - %(asctime)s - %(message)s')
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    return logger
logger = configure_logger(MODULE_NAME)


def create_postgres_table(table_name):
    """
    This function creates a table entered by the user in Postgres by executing the
    respective DDL creation script from the ddl_utils directory

    Args:
        table_name: Table to be created
    """
    try:
        # Establish connection
        logger.info("Creating '{table_name}' in Postgres...".format(table_name=table_name))
        conn = psycopg2.connect(
            database = constants.POSTGRES_CREDENTIALS["db_name"], 
            user = constants.POSTGRES_CREDENTIALS["username"], 
            host = constants.POSTGRES_CREDENTIALS["hostname"],
            password = constants.POSTGRES_CREDENTIALS["password"],
            port = constants.POSTGRES_CREDENTIALS["port"]
        )
        cur = conn.cursor()
        # Create table from DDL create file
        cur.execute(open("{stg_tables_base_path}{table_name}_create_ddl.sql".format(stg_tables_base_path=constants.STG_TABLES_BASE_PATH, table_name=table_name)).read())
        conn.commit()
    except Exception as e:
        logger.error("Table creation failed for '{table_name}'".format(table_name=table_name))
        raise e
    finally:
        cur.close()
        conn.close()


def create_df_from_processed_records(table_name, processed_records):
    """
    This function takes a list of processed records in the form of a dictionary and
    creates a DataFrame with the columns ordered in a list specified by the user.

    Args:
        processed_records: The list of dictionaries containing the processed records
        column_order: Order in which columns should be arranged in the output DataFrame

    Returns:
        DataFrame with the columns ordered in the specified order 
    """
    try:
        logger.info("Creating DataFrame for '{table_name}' records...".format(table_name=table_name))
        # Create DataFrame with records
        processed_records_df = None
        processed_records_df = pd.DataFrame(processed_records)
        processed_records_df = processed_records_df.reset_index(drop=True)
        column_mapper_dict = {}

        # Rename columns to snake case
        for column in processed_records_df.columns:
            column_mapper_dict[column] = ''.join(['_' + char.lower() if char.isupper() else char for char in column]).lstrip('_')
        processed_records_df = processed_records_df.rename(columns=column_mapper_dict)

        # Replace empty string and white spaces with 'null' values
        processed_records_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        return processed_records_df
    except Exception as e:
        logger.error("DataFrame creation failed for '{table_name}'".format(table_name=table_name))
        raise e


def insert_df_into_postgres_table(table_name, dataframe):
    """
    This function inserts the records into the Postgres table that was created. 

    Args:
        table_name: Table in which records are to be inserted
        dataframe: DataFrame containing the records to be inserted
    """
    try:
        logger.info("Inserting data into '{table_name}' table in Postgres...".format(table_name=table_name))
        # Establish Postgres connection
        connection_string = constants.POSTGRES_CONNECTION_STRING
        engine = sqlalchemy.create_engine(connection_string)

        # Get order of columns reindex DataFrame columns to match Postgres table
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name, metadata, autoload_with=engine)
        columns = table.columns.keys()
        dataframe = dataframe.reindex(columns=columns)

        # Write to Postgres
        dataframe.to_sql(table_name, engine, if_exists="append", index=False)
    except Exception as e:
        logger.error("Records insertion failed for '{table_name}'".format(table_name=table_name))
        raise e
    finally:
        engine.dispose()


def ingest_data(table_name, processed_records):
    """
    This function is resposible for ingesting the data processed by each of the individual modules 
    into Postgres. 

    Args:
        table_name: Name of the table which is being ingested
        processed_records: The processed records (list of dictionaries) to be ingested
    """

    try:
        # Create DataFrames from processed records
        table_df = create_df_from_processed_records(table_name, processed_records)
        logger.info("Successfully created DataFrame for '{table}'".format(table=table_name))

        # Create table in Postgres using DDL script
        create_postgres_table(table_name)
        logger.info("Successfully created '{table}' table in Postgres".format(table=table_name))

        # Insert records into table in Postgres
        insert_df_into_postgres_table(table_name, table_df)
        logger.info("Successfully inserted records into '{table}' table".format(table=table_name))

    except Exception as e:
        logger.error("Ingestion task for '{table}' failed".format(table=table_name))
        raise e


