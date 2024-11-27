from azure.storage.blob import BlobServiceClient
import requests

# Azure Data Lake connection details
STORAGE_ACCOUNT_NAME = ""  
STORAGE_ACCOUNT_KEY = "" 
CONTAINER_NAME = "" 

# Base URL for the trip data
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
file_pattern = "yellow_tripdata_2024-{:02d}.parquet"  # Format for months (01 to 12)

# Azure Blob Service client
blob_service_client = BlobServiceClient(
    f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)

def list_existing_files():
    """
    Lists all files currently in the 'raw' folder of Azure Data Lake.
    """
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs(name_starts_with="raw/")
    existing_files = [blob.name.split("/")[-1] for blob in blobs]  # Extract file names
    return existing_files

def upload_new_files():
    """
    Downloads Yellow Taxi Trip Data files for 2024 and uploads only new files to Azure Data Lake.
    """
    # List existing files in Data Lake
    existing_files = list_existing_files()
    print(f"Existing files in Data Lake: {existing_files}")

    for month in range(1, 13):  # Check all months (1 to 12)
        file_name = file_pattern.format(month)
        file_url = base_url + file_name

        # Skip if file already exists
        if file_name in existing_files:
            print(f"File already exists in Data Lake, skipping: {file_name}")
            continue

        try:
            # Download the file
            print(f"Downloading {file_url}...")
            response = requests.get(file_url)
            response.raise_for_status()

            # Create blob client with the "raw/" prefix
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=f"raw/{file_name}")
            
            # Upload the file to Azure Data Lake
            blob_client.upload_blob(response.content, overwrite=True)
            print(f"File successfully uploaded to 'raw/' folder: {file_name}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {file_url}: {e}")
        except Exception as e:
            print(f"Failed to upload {file_name} to Data Lake: {e}")

# Run the function
upload_new_files()
