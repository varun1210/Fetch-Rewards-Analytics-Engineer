import gzip
import json
import sys

from constants import BRANDS_RAW_DATA_FILE_PATH as brands_raw_data_file_path

from ingestion_scripts.ingestion_utils import configure_logger
from ingestion_scripts.ingestion_utils import ingest_data


MODULE_NAME = "BRANDS INGESTION TASK"
logger = configure_logger(MODULE_NAME)


def process_brands_file(brands_raw_file_path):
    """
    This function preprocesses the raw brands gzip file and returns a list of dictionaries
    that contains the records to be ingested into the 'stg_brands' file. The preprocessing 
    involves converting the binary file into an ingestable format, and also some type 
    casting so that it can match the schemas defined in Postgres.

    Args:
        brands_raw_file_path: File path to the raw brands gzip file

    Returns:
        List of dictionaries containing processed records from brands gzip file
    """
    logger.info("Processing brands gzip file...")
    try:
        stg_brands_records = []

        # open file and convert to parsable format
        with gzip.open(brands_raw_file_path, 'rb') as file:
            file_content = file.read()
            decoded_file = file_content.decode("utf-8")
            decoded_file = decoded_file.split("\n")
            stg_brands_column_list = ["brand_id", "barcode", "brandCode", "category", "categoryCode", "name", "topBrand", "cpgId", "cpgRef"]

            # Preprocessing
            for record in decoded_file:
                if(not record):
                     continue
                record = json.loads(record)
                if("_id" in record and "$oid" in record["_id"]):
                    record["brand_id"] = record["_id"]["$oid"]
                if("cpg" in record and "$id" in record["cpg"] and "$oid" in record["cpg"]["$id"]):
                    record["cpgId"] = record["cpg"]["$id"]["$oid"]
                if("cpg" in record and "$ref" in record["cpg"]):
                    record["cpgRef"] = record["cpg"]["$ref"]
                for field in record:
                    if type(record[field]) == str:
                        record[field] = record[field].strip()
                processed_record = {key: record[key] for key in stg_brands_column_list if key in record}
                stg_brands_records.append(processed_record)
        return stg_brands_records
    except Exception as e:
        logger.error("Error processing brands gzip file")
        raise e
    finally:
        file.close()
    
def create_stg_brands():
    try:
        logger.info("Creating and ingesting data into 'stg_brands'...")

        # Processing brands gzip file
        stg_brands_data = process_brands_file(brands_raw_data_file_path)
        logger.info("Successfully processed brands gzip file")
        if(len(stg_brands_data) == 0):
            logger.warning("0 records in gzip file for 'stg_brands'")

        # Ingesting the data
        ingest_data("stg_brands", stg_brands_data)
        logger.info("Successfully completed ingestion task for 'stg_brands'")
    except Exception as e:
        logger.info("Creation and ingestion task for 'stg_brands' failed")
        raise e











    
