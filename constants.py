# IDEALLY WOULD BE STORED AS ENVIRONMENT VARIABLES DURING DEPLOYMENT
# EDIT YOUR POSTGRES CREDENTIALS
# EDIT ONLY THE VALUES IN THIS DICTIONARY, DO NOT EDIT KEY NAMES
POSTGRES_CREDENTIALS = {
    "db_name" : "fetch_analytics_engineer", # EDIT
    "username" : "postgres", # EDIT
    "hostname" : "localhost", # EDIT
    "password" : "varun", # EDIT
    "port" : 5433 # EDIT
}

# DO NOT NEED TO EDIT THIS FILE BEYOND THIS POINT 
POSTGRES_CONNECTION_STRING = "postgresql://{username}:{password}@{hostname}:{port}/{db_name}".format(username=POSTGRES_CREDENTIALS["username"], password=POSTGRES_CREDENTIALS["password"], hostname=POSTGRES_CREDENTIALS["hostname"], port=POSTGRES_CREDENTIALS["port"], db_name=POSTGRES_CREDENTIALS["db_name"])

RECEIPTS_RAW_DATA_FILE_PATH = "./raw_data/receipts.json.gz"
USERS_RAW_DATA_FILE_PATH = "./raw_data/users.json.gz"
BRANDS_RAW_DATA_FILE_PATH = "./raw_data/brands.json.gz"

STG_TABLES_BASE_PATH = "./stg_table_ddl/"
SRC_TABLES_BASE_PATH = "./src_table_ddl/"

