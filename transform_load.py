# Set the storage account key
STORAGE_ACCOUNT_NAME = dbutils.widgets.get("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = dbutils.widgets.get("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = dbutils.widgets.get("CONTAINER_NAME")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

# Import Required Libraries
import json
from pyspark.sql.functions import col, when, unix_timestamp

# Construct raw data path and processed metadata path dynamically
RAW_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/"
PROCESSED_METADATA_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/processed_files.json"
PROCESSED_COMBINED_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/processed_combined/"

print(f"Raw Data Path: {RAW_PATH}")
print(f"Metadata Path: {PROCESSED_METADATA_PATH}")
print(f"Processed Combined Path: {PROCESSED_COMBINED_PATH}")

# Load Metadata for Processed Files
try:
    processed_files_json = spark.read.text(PROCESSED_METADATA_PATH).collect()[0][0]
    processed_files = json.loads(processed_files_json)["processed_files"]
except Exception as e:
    # If metadata file doesn't exist or is empty, initialize an empty list
    processed_files = []

# Identify New Files
all_files = [file.name for file in dbutils.fs.ls(RAW_PATH) if file.name.endswith(".parquet")]
new_files = [file for file in all_files if file not in processed_files]

# Stop if no new files to process
if not new_files:
    print("No new files to process.")
else:
    print(f"Processing new files: {new_files}")

    # Load and Process New Files
    new_files_paths = [f"{RAW_PATH}{file}" for file in new_files]
    df = spark.read.parquet(*new_files_paths)

    # Data Cleaning and Transformation
    df = df.filter((col("trip_distance") > 0) & (col("total_amount") > 0))
    df = df.filter(unix_timestamp("tpep_dropoff_datetime") > unix_timestamp("tpep_pickup_datetime"))
    df = df.select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "trip_distance", "PULocationID", "DOLocationID", "tip_amount", "total_amount"
    ).withColumn(
        "trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    ).withColumn(
        "tip_ratio", col("tip_amount") / col("total_amount")
    ).withColumn(
        "distance_segment",
        when(col("trip_distance") <= 2, "short")
        .when(col("trip_distance") <= 10, "medium")
        .otherwise("long")
    )

    # Append to Existing CSV File
    try:
        # Load existing data if it exists
        existing_df = spark.read.csv(PROCESSED_COMBINED_PATH, header=True, inferSchema=True)
        combined_df = existing_df.union(df)
    except Exception as e:
        # If no existing data, use new data as combined data
        combined_df = df

    # Save the combined data back to the same location
    combined_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(PROCESSED_COMBINED_PATH)
    print(f"Data successfully saved to {PROCESSED_COMBINED_PATH}")

    # Update Metadata
    processed_files.extend(new_files)
    updated_metadata = {"processed_files": processed_files}
    dbutils.fs.put(PROCESSED_METADATA_PATH, json.dumps(updated_metadata), overwrite=True)
    print("Metadata file updated.")
