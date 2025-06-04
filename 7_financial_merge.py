# import pandas as pd
# import sys

# sys.path.append('/app/')
# sys.path.append('/app/TransformData/VietNam')
# from TransformData.VietNam.base.PATH_UPDATE import *
# from VAR_GLOBAL_CONFIG import *
# from TransformData.VietNam.base.Financial import CafeF,VietStock
# from TransformData.VietNam.base.Setup import *
# from Flow.ulis import *


# Data = pd.read_excel(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}_with_FIX.xlsx",sheet_name="Sheet1")

# current = 0
# def read_file(path):
#     try:
#         return pd.read_csv(path)
#     except:
#         return pd.DataFrame()

# def alalyst_code(code):
#     try:
#         source = None
#         code = int(code)
#         if code == 1:
#             source = "CafeF"
#         elif code == 2 or code ==0:
#             source = "FileFix"
#         else:
#             pass
#         return source
#     except ValueError:
#         return

# def getDataFixError(x,y,source):
#     if source == "CafeF":
#         return x
#     elif source == "FileFix":
#         return y
#     return None


# Data_Source = Data
# DATA = pd.DataFrame()

# for com in SYMBOL:
#     current += 1

#     # Filter data for the current symbol
#     df = Data_Source[Data_Source["Symbol"] == com]
    
#     # Ensure the "Feature" column is unique
#     if not df["Feature"].is_unique:
#         df["Feature"] = df["Feature"] + "_" + df.groupby("Feature").cumcount().astype(str)
    
#     # Select only the necessary columns and reset index
#     df = df[["Feature", "FIX"]].reset_index(drop=True).T

#     # Rename columns using the first row
#     df = df.rename(columns=df.iloc[0])
    
#     # Drop the first row (now column names) and reset index
#     df = df.drop(df.index[0]).reset_index(drop=True)

#     # Add the 'Symbol' column
#     df["Symbol"] = [com for i in df.index]

#     # Debug: Check for duplicates or mismatched columns
#     print(f"Processing {com}: df columns = {df.columns}")

#     # Concatenate the transformed DataFrame
#     DATA = pd.concat([DATA, df], ignore_index=True)
    
#     # Display progress
#     progress_bar(current, TOTAL, text="Bien doi hang")

# YEAR_KEY = '_'.join(YEAR_KEY) 
# DATA["Time"] = [YEAR_KEY for i in DATA.index]
# DATA.to_excel(f"{FU.PATH_MAIN_CURRENT}/FINANCAIL_{YEAR_KEY.replace('/','_')}.xlsx",index=False)
# print("Done financial merge")

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
# INPUT_FILE_KEY = "data/Ingestion/Compare/Financial_Year_with_FIX.xlsx"
INPUT_FILE_KEY = "data/Ingestion/Compare/Financial_Year_with_FIX_1.xlsx"
# OUTPUT_FILE_NAME = "FINANCIAL_2023.xlsx"
OUTPUT_FILE_NAME = "FINANCIAL_2023_1.xlsx"
LOCAL_INPUT_FILE = f"/tmp/Financial_Year_with_FIX_1.xlsx"
LOCAL_OUTPUT_FILE = f"/tmp/{OUTPUT_FILE_NAME}"
MINIO_OUTPUT_KEY = f"data/Ingestion/{OUTPUT_FILE_NAME}"

# Download from MinIO
def download_from_minio(bucket, key, local_path):
    """
    Download a file from MinIO to a local path.
    """
    try:
        with open(local_path, 'wb') as f:
            minio_client.download_fileobj(bucket, key, f)
        print(f"Downloaded {key} from MinIO to {local_path}")
    except Exception as e:
        print(f"Error downloading {key} from MinIO: {e}")
        raise

# Upload to MinIO
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
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        print(f"Uploaded {local_file_path} to MinIO at {key}")
    except Exception as e:
        print(f"Error uploading {local_file_path} to MinIO: {e}")
        raise

# Process Financial Data Fix
def process_financial_fix(input_file, output_file):
    """
    Process Financial_Year_with_FIX.xlsx to create FINANCIAL_2023.xlsx.
    """
    # Read Input File
    print(f"Reading input file: {input_file}")
    data = pd.read_excel(input_file, sheet_name="Sheet1")

    current = 0
    total = len(data['Symbol'].unique())  # Total number of unique symbols
    DATA = pd.DataFrame()

    # Processing each symbol
    for com in tqdm(data['Symbol'].unique(), total=total, desc="Processing Symbols"):
        # Filter data for the current symbol
        df = data[data["Symbol"] == com]

        # Ensure the "Feature" column is unique
        if not df["Feature"].is_unique:
            df["Feature"] = df["Feature"] + "_" + df.groupby("Feature").cumcount().astype(str)

        # Transform the DataFrame
        df = df[["Feature", "FIX"]].reset_index(drop=True).T
        df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)
        df["Symbol"] = com

        # Concatenate the transformed DataFrame
        DATA = pd.concat([DATA, df], ignore_index=True)

    # Add Time Column
    YEAR_KEY = "2023"
    DATA["Time"] = YEAR_KEY

    # Save the result
    print(f"Saving processed file to: {output_file}")
    DATA.to_excel(output_file, index=False)

# Main Execution
if __name__ == "__main__":
    try:
        # Step 1: Download the input file from MinIO
        download_from_minio(MINIO_BUCKET, INPUT_FILE_KEY, LOCAL_INPUT_FILE)

        # Step 2: Process the financial fix
        process_financial_fix(LOCAL_INPUT_FILE, LOCAL_OUTPUT_FILE)

        # Step 3: Upload the processed file to MinIO
        upload_to_minio(MINIO_BUCKET, MINIO_OUTPUT_KEY, LOCAL_OUTPUT_FILE)
        print(f"File successfully processed and uploaded to MinIO: {MINIO_OUTPUT_KEY}")
    except Exception as e:
        print(f"An error occurred: {e}")
