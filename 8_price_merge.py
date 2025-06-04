# import pandas as pd
# import sys

# sys.path.append('/app/')
# sys.path.append('/app/TransformData/VietNam')
# from TransformData.VietNam.base.PATH_UPDATE import *
# from VAR_GLOBAL_CONFIG import *
# from TransformData.VietNam.base.Setup import *
# from Flow.ulis import *

# CURRENT = 0
# VALUE = pd.DataFrame()
# for symbol in SYMBOL:
#     CURRENT+=1 
#     try:
#         df = pd.read_csv(FU.joinPath(FU.PATH_MAIN_CURRENT,"Close","CafeF","F1",f"{symbol}.csv"))
#         df["Symbol"] = [symbol for i in df.index]
#     except:
#         print(symbol)
#         continue
#     VALUE = pd.concat([VALUE,df], ignore_index=True)
#     progress_bar(CURRENT,TOTAL,text="Gom Giá")

# VALUE.to_csv(f"{FU.PATH_MAIN_CURRENT}/VALUE_ARG.csv",index=False)
# print("Done price merge")

import pandas as pd
import os
import boto3
from tqdm import tqdm  # For progress bar

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

# File Paths
MINIO_INPUT_PATH = "data/Ingestion/Close/CafeF/F1"
OUTPUT_FILE_NAME = "VALUE_ARG.csv"
LOCAL_OUTPUT_FILE = f"/tmp/{OUTPUT_FILE_NAME}"
MINIO_OUTPUT_KEY = f"data/Ingestion/{OUTPUT_FILE_NAME}"

# Helper Functions
def download_csv_from_minio(bucket, key):
    """
    Download a CSV file from MinIO and return it as a DataFrame.
    """
    try:
        response = minio_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        return df
    except Exception as e:
        print(f"Error downloading {key} from MinIO: {e}")
        return pd.DataFrame()

def upload_to_minio(bucket, key, local_file_path):
    """
    Upload a file to MinIO.
    """
    try:
        with open(local_file_path, "rb") as file_data:
            minio_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=file_data,
                ContentType="text/csv"
            )
        print(f"Uploaded {local_file_path} to MinIO at {key}")
    except Exception as e:
        print(f"Error uploading {local_file_path} to MinIO: {e}")
        raise

# Main Processing
def process_price_merge():
    """
    Process F1 files from MinIO and merge them into a single CSV file.
    """
    print("Starting price merge process...")

    VALUE = pd.DataFrame()
    SYMBOLS = [obj['Key'].split('/')[-1].replace('.csv', '') for obj in minio_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_INPUT_PATH)['Contents'] if obj['Key'].endswith('.csv')]

    total_symbols = len(SYMBOLS)
    for symbol in tqdm(SYMBOLS, total=total_symbols, desc="Merging Prices"):
        file_key = f"{MINIO_INPUT_PATH}/{symbol}.csv"
        try:
            df = download_csv_from_minio(MINIO_BUCKET, file_key)
            if not df.empty:
                df["Symbol"] = symbol
                VALUE = pd.concat([VALUE, df], ignore_index=True)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    # Save the merged data locally
    print(f"Saving merged data to: {LOCAL_OUTPUT_FILE}")
    VALUE.to_csv(LOCAL_OUTPUT_FILE, index=False)

    # Upload the merged file back to MinIO
    upload_to_minio(MINIO_BUCKET, MINIO_OUTPUT_KEY, LOCAL_OUTPUT_FILE)
    print(f"Price merge completed. File saved to MinIO: {MINIO_OUTPUT_KEY}")

# Execute the script
if __name__ == "__main__":
    process_price_merge()
