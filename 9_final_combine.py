# import pandas as pd
# import sys
# sys.path.append('/app/')
# sys.path.append('/app/TransformData/VietNam')
# from TransformData.VietNam.base.PATH_UPDATE import *
# from VAR_GLOBAL_CONFIG import *
# from TransformData.VietNam.base.Setup import *
# from Flow.ulis import *


# financial_df = pd.read_excel(f"{FU.PATH_MAIN_CURRENT}/FINANCAIL_2023.xlsx",sheet_name="Sheet1")
# value_df = pd.read_csv(f"{FU.PATH_MAIN_CURRENT}/VALUE_ARG.csv")
# # output_file = "f"{FU.PATH_MAIN_CURRENT}/MERGED_FINANCIAL_VALUE.xlsx""

# # Load the financial data
# # financial_df = pd.read_excel(financial_file)
# print("Loaded FINANCAIL_2023.xlsx:")
# print(financial_df.head())

# # Load the value data
# # value_df = pd.read_csv(value_file)
# print("Loaded VALUE_ARG.csv:")
# print(value_df.head())

# # Rename the 'Time' column to 'Time_Close'
# value_df = value_df.rename(columns={"Time": "Time_Close"})
# print("Renamed 'Time' to 'Time_Close':")
# print(value_df.head())

# # Merge the two dataframes on the 'Symbol' column
# merged_df = pd.merge(financial_df, value_df, on="Symbol", how="outer")
# print("Merged DataFrame:")
# print(merged_df.head())

# # Save the merged result to an Excel file
# merged_df.to_csv(f"{FU.PATH_MAIN_CURRENT}/MERGED_FINANCIAL_VALUE.csv", index=False)
# print("Merged data saved")
import pandas as pd
import os
import boto3

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize MinIO client
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def download_from_minio(bucket, key, local_path):
    """
    Download a file from MinIO to a local path.
    """
    with open(local_path, 'wb') as f:
        minio_client.download_fileobj(bucket, key, f)
    print(f"Downloaded {key} from MinIO to {local_path}")

def upload_to_minio(local_path, bucket, key):
    """
    Upload a file from a local path to MinIO.
    """
    with open(local_path, "rb") as file_data:
        minio_client.put_object(Bucket=bucket, Key=key, Body=file_data, ContentType="text/csv")
    print(f"Uploaded {key} to MinIO.")


# FINANCIAL_FILE_KEY = "data/Ingestion/FINANCIAL_2023.xlsx"
FINANCIAL_FILE_KEY = "data/Ingestion/FINANCIAL_2023_1.xlsx"
# VALUE_FILE_KEY = "data/Ingestion/VALUE_ARG.csv"
VALUE_FILE_KEY = "data/Ingestion/VALUE_ARG_1.csv"
# MERGED_FILE_KEY = "data/Ingestion/MERGED_FINANCIAL_VALUE.csv"
MERGED_FILE_KEY = "data/Ingestion/MERGED_FINANCIAL_VALUE_1.csv"


# LOCAL_FINANCIAL_FILE = "/tmp/FINANCIAL_2023.xlsx"
# LOCAL_VALUE_FILE = "/tmp/VALUE_ARG.csv"
# LOCAL_MERGED_FILE = "/tmp/MERGED_FINANCIAL_VALUE.csv"
LOCAL_FINANCIAL_FILE = "/tmp/FINANCIAL_2023_1.xlsx"
LOCAL_VALUE_FILE = "/tmp/VALUE_ARG_1.csv"
LOCAL_MERGED_FILE = "/tmp/MERGED_FINANCIAL_VALUE_1.csv"

# Step 1: Download necessary files from MinIO
download_from_minio(MINIO_BUCKET, FINANCIAL_FILE_KEY, LOCAL_FINANCIAL_FILE)
download_from_minio(MINIO_BUCKET, VALUE_FILE_KEY, LOCAL_VALUE_FILE)

# Step 2: Load the files into DataFrames
financial_df = pd.read_excel(LOCAL_FINANCIAL_FILE, sheet_name="Sheet1")
value_df = pd.read_csv(LOCAL_VALUE_FILE)

# Step 3: Transform and merge
value_df = value_df.rename(columns={"Time": "Time_Close"})
merged_df = pd.merge(financial_df, value_df, on="Symbol", how="outer")

# Step 4: Save the merged DataFrame locally
merged_df.to_csv(LOCAL_MERGED_FILE, index=False)
print("Merged data saved locally.")

# Step 5: Upload the merged file back to MinIO
upload_to_minio(LOCAL_MERGED_FILE, MINIO_BUCKET, MERGED_FILE_KEY)
print("Merged data uploaded to MinIO.")
