%python

STORAGE_ACCOUNT_NAME = dbutils.widgets.get("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = dbutils.widgets.get("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = dbutils.widgets.get("CONTAINER_NAME")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)
import json

# Define paths for metadata file
METADATA_PATH = f"dbfs:/mnt/{CONTAINER_NAME}/processed_files.json"
ADLS_METADATA_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/processed_files.json"

# Create or verify the existence of the metadata file
try:
    # Check if the metadata file already exists
    dbutils.fs.ls(ADLS_METADATA_PATH)
    print("Metadata file already exists. Skipping creation.")
except Exception as e:
    # If the metadata file does not exist, create an empty file
    processed_files = {"processed_files": []}
    json_content = json.dumps(processed_files)

    # Write the file to DBFS
    dbutils.fs.put(METADATA_PATH, json_content, overwrite=True)

    # Copy the metadata file to Azure Data Lake
    dbutils.fs.cp(METADATA_PATH, ADLS_METADATA_PATH)
    print("Empty metadata file created successfully: processed_files.json")

