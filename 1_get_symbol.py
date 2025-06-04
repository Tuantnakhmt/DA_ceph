# import pandas as pd
# import sys
# sys.path.append('/app/')

# from datetime import datetime
# # from Flow.Folder import FolderCrawl, FolderData, FolderUpdate
# # from Flow.PATH_env import PATH_ENV
# from VAR_GLOBAL_CONFIG import *

# # FC = FolderCrawl()
# # FU = FolderUpdate(date=END_DAY_UPDATE)

# def GetListSymbol(FROM,TO):
#     PATH_FROM = FC.joinPath(FC.PATH_MAIN,FROM,"List_company.csv")
#     PATH_TO = FU.joinPath(FU.PATH_MAIN,TO,"List_company.csv")
#     print(PATH_FROM,PATH_TO)
#     pd.read_csv(PATH_FROM).to_csv(PATH_TO,index=False)
# FROM = FC.GetDateUpdate(START_DAY_LIST_UPDATE)
# TO = FU.GetDateUpdate(END_DAY_UPDATE)
# GetListSymbol(FROM,TO)
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

# MinIO Path
MINIO_SYMBOL_FILE = "data/Ingestion/Close/CafeF/List_company.csv"
LOCAL_SYMBOL_FILE = "/tmp/List_company.csv"

def download_from_minio(bucket, key, local_path):
    """
    Download a file from MinIO to a local path.
    """
    with open(local_path, 'wb') as f:
        minio_client.download_fileobj(bucket, key, f)
    print(f"Downloaded {key} from MinIO to {local_path}")

def GetListSymbol(FROM, TO):
    """
    Mimic the original function by reading the List_company file from MinIO.
    """
    # Step 1: Download List_company.csv from MinIO
    download_from_minio(MINIO_BUCKET, MINIO_SYMBOL_FILE, LOCAL_SYMBOL_FILE)

    # Step 2: Process the file
    print(f"Processing symbols from {FROM} to {TO}")
    symbols_df = pd.read_csv(LOCAL_SYMBOL_FILE)
    symbols_df.to_csv(LOCAL_SYMBOL_FILE, index=False)  # Re-save locally if necessary
    print(f"Symbols processed and saved locally at {LOCAL_SYMBOL_FILE}")

if __name__ == "__main__":
    FROM = "2024-12-20"  # Replace with actual logic to determine FROM
    TO = "2024-12-20"    # Replace with actual logic to determine TO
    GetListSymbol(FROM, TO)
