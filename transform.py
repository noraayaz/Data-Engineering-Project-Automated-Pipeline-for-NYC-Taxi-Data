%python
# Set up the configuration for accessing the Azure storage account
spark.conf.set("fs.azure.account.key.STORAGEACCOUNT.dfs.core.windows.net", "ACCOUNTKEY")

# Paths to raw data for 2015 and 2024
raw_path_2015 = "abfss://nyc-taxi-data@mystorageaccount1089.dfs.core.windows.net/yellow_tripdata_2015-01.csv"
raw_path_2024 = "abfss://nyc-taxi-data@mystorageaccount1089.dfs.core.windows.net/raw/yellow_tripdata_2024-*.parquet"

# Load the 2015 data (CSV format)
# header=True: Treat the first row as column names
# inferSchema=True: Automatically infer column data types
df_2015 = spark.read.csv(raw_path_2015, header=True, inferSchema=True)

# Load the 2024 data (Parquet format)
df_2024 = spark.read.parquet(raw_path_2024)

# Add missing columns to the 2015 dataset with null values
for column in df_2024.columns:
    if column not in df_2015.columns:
        df_2015 = df_2015.withColumn(column, lit(None).cast(df_2024.schema[column].dataType))

# Align columns: Ensure both datasets have the same column names and order
df_2015 = df_2015.select(*df_2024.columns)

# Combine the datasets using unionByName (column-based union)
combined_df = df_2015.unionByName(df_2024)

# Display the first 5 rows of the combined dataset
display(combined_df.limit(5))