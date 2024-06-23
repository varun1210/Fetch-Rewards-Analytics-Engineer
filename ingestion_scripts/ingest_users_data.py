import gzip
import json
import datetime
import sys

from constants import USERS_RAW_DATA_FILE_PATH as users_raw_data_file_path

from ingestion_scripts.ingestion_utils import configure_logger
from ingestion_scripts.ingestion_utils import ingest_data


MODULE_NAME = "USERS INGESTION TASK"
logger = configure_logger(MODULE_NAME)


def process_users_file(users_raw_file_path):
    """
    This function preprocesses the raw users gzip file and returns a list of dictionaries
    that contains the records to be ingested into the 'stg_users' file. The preprocessing 
    involves converting the binary file into an ingestable format, and also some type 
    casting so that it can match the schemas defined in Postgres.

    Args:
        users_raw_file_path: File path to the raw users gzip file

    Returns:
        List of dictionaries containing processed records from users gzip file
    """
    logger.info("Processing users gzip file...")
    try:
        stg_users_records = []

        with gzip.open(users_raw_file_path, 'rb') as file:
            file_content = file.read().strip(b"\x00")
            decoded_file = file_content.decode("utf-8")
            decoded_file = decoded_file.split("\n")
            stg_users_column_list = ["user_id", "state", "createdDate", "lastLogin", "role", "active", "signUpSource"]
            temporal_columns = ["createdDate", "lastLogin"]

            # Preprocessing
            decoded_file[0] = """{"_id":{"$oid":"5ff1e194b6a9d73a3a9f1052"},"active":true,"createdDate":{"$date":1609687444800},"lastLogin":{"$date":1609687537858},"role":"consumer","signUpSource":"Email","state":"WI"}"""
            for record in decoded_file:
                if(not record):
                    continue
                record = json.loads(record)
                if "_id" in record and "$oid" in record["_id"]:
                    record["user_id"] = record["_id"]["$oid"]
                for column in temporal_columns:
                    if column in record and "$date" in record[column]:
                        record[column] = str(datetime.datetime.fromtimestamp(record[column]["$date"] / 1000))
                for field in record:
                    if type(record[field]) == str:
                        record[field] = record[field].strip()
                processed_record = {key: record[key] for key in stg_users_column_list if key in record}
                stg_users_records.append(processed_record)
        return stg_users_records
    except Exception as e:
        logger.error("Error processing users gzip file")
        raise e
    finally:
        file.close()
    
def create_stg_users():
    try:
        logger.info("Creating and ingesting data into 'stg_users'...")

        # Processing users gzip file
        stg_users_data = process_users_file(users_raw_data_file_path)
        logger.info("Successfully processed brands gzip file")
        if(len(stg_users_data) == 0):
            logger.warning("0 records in gzip file for 'stg_brands'")

        # Ingesting the data
        ingest_data("stg_users", stg_users_data)
        logger.info("Successfully completed ingestion task for 'stg_users'")
    except Exception as e:
        logger.info("Creation and ingestion task for 'stg_brands' failed")
        raise e
        