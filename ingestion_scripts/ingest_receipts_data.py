import gzip
import json
import datetime
import sys

from constants import RECEIPTS_RAW_DATA_FILE_PATH as receipts_raw_data_file_path

from ingestion_scripts.ingestion_utils import configure_logger
from ingestion_scripts.ingestion_utils import ingest_data


MODULE_NAME = "RECEIPTS INGESTION TASK"
logger = configure_logger(MODULE_NAME)


def process_receipts_file(receipts_raw_file_path):
    """
    This function preprocesses the raw receipts gzip file and returns two lists of dictionaries
    that contain the records to be ingested into the 'stg_receipts' and 'stg_receipts_items' tables. 
    The preprocessing involves converting the binary file into an ingestable format, and also some 
    type casting so that it can match the schemas defined in Postgres.

    Args:
        receipts_raw_file_path: File path to the raw receipts gzip file

    Returns:
        Two lists of dictionaries.
         - First dictionary contains records of 'stg_receipts' table
         - Second dictionary contains records of 'stg_receipts_items' table
    """
    file = None
    try:
        logger.info("Processing receipts gzip file...")
        stg_receipts_records = []
        stg_receipts_items_records = []

        # open file and convert to parsable format
        with gzip.open(receipts_raw_file_path, 'rb') as file:
            file_content = file.read()
            decoded_file = file_content.decode("utf-8")
            decoded_file = decoded_file.split("\n")
            
            # Nested function to process "rewardsReceiptItemList" key in the dictionary. This 
            # function generates records for 'stg_receipt_items' table
            def process_receipt_items(receipt_items_list, receipt_id):
                try: 
                    # Preprocessing
                    for item in receipt_items_list:
                        item["receiptId"] = receipt_id
                        if('finalPrice' in item and item["finalPrice"]):
                            item['finalPrice'] = float(item['finalPrice'])
                        if('itemPrice' in item and item["itemPrice"]):
                            item['itemPrice'] = float(item['itemPrice'])
                        if('originalFinalPrice' in item and item["originalFinalPrice"]):
                            item['originalFinalPrice'] = float(item['originalFinalPrice'])
                        if('originalMetaBriteItemPrice' in item and item["originalMetaBriteItemPrice"]):
                            item['originalMetaBriteItemPrice'] = float(item['originalMetaBriteItemPrice'])
                        if('originalMetaBriteQuantityPurchased' in item and item["originalMetaBriteQuantityPurchased"]):
                            item['originalMetaBriteQuantityPurchased'] = int(item['originalMetaBriteQuantityPurchased'])
                        if('pointsEarned' in item and item["pointsEarned"]):
                            item['pointsEarned'] = float(item['pointsEarned'])
                        if('quantityPurchased' in item and item["quantityPurchased"]):
                            item['quantityPurchased'] = int(item['quantityPurchased'])
                        if('targetPrice' in item and item["targetPrice"]):
                            item['targetPrice'] = float(item['targetPrice'])
                        if('userFlaggedPrice' in item and item["userFlaggedPrice"]):
                            item['userFlaggedPrice'] = float(item['userFlaggedPrice'])
                        if('userFlaggedQuantity' in item and item["userFlaggedQuantity"]):
                            item['userFlaggedQuantity'] = int(item['userFlaggedQuantity'])
                        
                        for field in item:
                            if type(item[field]) == str:
                                item[field] = item[field].strip()
                    return receipt_items_list
                except Exception as e:
                    logger.error("Error processing receipt items. receipt_id : {receipt_id}".format(receipt_id=receipt_id))
                    raise e

            # Preprocessing
            stg_receipts_column_list = ["receipt_id", "bonusPointsEarned", "bonusPointsEarnedReason", "createDate", "dateScanned", "finishedDate", "modifyDate", "pointsAwardedDate", "pointsEarned", "purchaseDate", "purchasedItemCount", "rewardsReceiptStatus", "totalSpent", "userId", "containsItems"]
            temporal_columns = ["createDate", "dateScanned", "finishedDate", "modifyDate", "pointsAwardedDate", "purchaseDate"]
            for record in decoded_file:
                if(not record):
                    continue
                record = json.loads(record)
                if("_id" in record and "$oid" in record["_id"]):
                    record["receipt_id"] = record["_id"]["$oid"]
                if("_id" not in record):
                    record["receipt_id"] = None
                record["containsItems"] = True if "rewardsReceiptItemList" in record else False
                if(record["containsItems"]):
                    stg_receipts_items_records += process_receipt_items(record["rewardsReceiptItemList"], record["receipt_id"])
                for column in temporal_columns:
                    if(column in record and "$date" in record[column]):
                        record[column] = str(datetime.datetime.fromtimestamp(record[column]["$date"] / 1000))
                if("bonusPointsEarned" in record and record["bonusPointsEarned"]):
                    record["bonusPointsEarned"] = float(record["bonusPointsEarned"])
                if("pointsEarned" in record and record["pointsEarned"]):
                    record["pointsEarned"] = float(record["pointsEarned"])
                if("purchasedItemCount" in record and record["purchasedItemCount"]):
                    record["purchasedItemCount"] = int(record["purchasedItemCount"])
                if("totalSpent" in record and record["totalSpent"]):
                    record["totalSpent"] = float(record["totalSpent"])
                if("discountedItemPrice" in record and record["discountedItemPrice"]):
                    record["discountedItemPrice"] = float(record["discountedItemPrice"])
                if("priceAfterCoupon" in record and record["priceAfterCoupon"]):
                    record["priceAfterCoupon"] = float(record["priceAfterCoupon"])
                for field in record:
                    if type(record[field]) == str:
                        record[field] = record[field].strip()
                processed_record = {key: record[key] for key in stg_receipts_column_list if key in record}
                stg_receipts_records.append(processed_record)
        return stg_receipts_records, stg_receipts_items_records
    
    except Exception as e:
        logger.error("Error processing receipts gzip file")
        raise e
    
    finally:
        if file:
            file.close()

def create_stg_receipts_and_stg_receipts_items():
    try:
        logger.info("Creating and ingesting data into 'stg_receipts' and 'stg_receipts_items'...")

        # Processing receipts gzip file
        stg_receipts_data, stg_receipts_items_data  = process_receipts_file(receipts_raw_data_file_path)
        logger.info("Successfully processed receipts gzip file")

        if(len(stg_receipts_data) == 0):
            logger.warning("0 records in gzip file for 'stg_receipts'")
        if(len(stg_receipts_items_data) == 0):
            logger.warning("0 records in gzip file for 'stg_receipts_items'")

        # Ingesting the data
        ingest_data("stg_receipts", stg_receipts_data)
        ingest_data("stg_receipts_items", stg_receipts_items_data)
        logger.info("Successfully completed ingestion task for 'stg_receipts' and 'stg_receipt_items'")
    except Exception as e:
        logger.info("Creation and ingestion task for 'stg_brands' failed")
        raise e
    