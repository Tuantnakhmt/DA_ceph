# import pandas as pd
# import sys
# import os
# sys.path.append('/app/')
# sys.path.append('/app/TransformData/VietNam')
# from TransformData.VietNam.base.PATH_UPDATE import *
# from VAR_GLOBAL_CONFIG import *
# from TransformData.VietNam.base.Financial import CafeF,VietStock
# from TransformData.VietNam.base.Setup import *
# from Flow.ulis import *

# print(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}.xlsx")
# df = pd.read_excel(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}.xlsx",sheet_name="Sheet1")

# def determine_fix(row):
#     """
#     Determine the value for the 'FIX' column based on the 'Compare' column:
#     - If Compare = 0, choose the value from 2023_x.
#     - If Compare = 1, get either from 2023_x or 2023_y (choose whichever is not NaN).
#     - If Compare = 2, remain as the original logic.
#     - Else, return "Skip".
#     """
#     if row['Compare'] == 0:
#         return row['2023_x']  # Always choose from 2023_x
#     elif row['Compare'] == 1:
#         # Choose from 2023_x or 2023_y (whichever is not NaN)
#         return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
#     elif row['Compare'] == 2:
#         # Original logic for Compare = 2
#         return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
#     else:
#         return "Skip"

# df['FIX'] = df.apply(determine_fix, axis=1)

# # print("First few rows of the DataFrame:")
# # print(df.head())

# print("Unique values in 'Compare' column:")
# print(df['Compare'].unique())

# print("Count of non-NaN values in '2023_x' and '2023_y':")
# print(f"2023_x: {df['2023_x'].notna().sum()}, 2023_y: {df['2023_y'].notna().sum()}")

# # Save the result to a new Excel file
# df.to_excel("Financial_Year_with_FIX.xlsx", index=False)
# # Save the result to the same folder as the input file
# output_file_path = os.path.join(PATH_COMPARE, f"{YEAR_FINANCAIL_FIX_FILE}_with_FIX.xlsx")
# df.to_excel(output_file_path, index=False)
# print(f"File saved at: {output_file_path}")

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

# File Paths
# INPUT_FILE_KEY = "data/Ingestion/Compare/Financial_Year.xlsx"
INPUT_FILE_KEY = "data/Ingestion/Compare/Financial_Year_pipeline.xlsx"
OUTPUT_FILE_NAME = "Financial_Year_with_FIX_1.xlsx"
# LOCAL_INPUT_FILE = f"/tmp/Financial_Year.xlsx"
LOCAL_INPUT_FILE = f"/tmp/Financial_Year_pipeline.xlsx"
LOCAL_OUTPUT_FILE = f"/tmp/{OUTPUT_FILE_NAME}"
MINIO_OUTPUT_KEY = f"data/Ingestion/Compare/{OUTPUT_FILE_NAME}"

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

# Process Financial Data to Add FIX Column
def process_financial_data(input_file, output_file):
    """
    Process Financial_Year.xlsx to add a FIX column and save the result.
    """
    # Read Input File
    print(f"Reading input file: {input_file}")
    df = pd.read_excel(input_file, sheet_name="Sheet1")

    # Define the logic for the FIX column
    def determine_fix(row):
        if row['Compare'] == 0:
            return row['2023_x']  # Always choose from 2023_x
        elif row['Compare'] == 1:
            return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
        elif row['Compare'] == 2:
            return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
        else:
            return "Skip"

    # Apply the FIX Logic
    df['FIX'] = df.apply(determine_fix, axis=1)

    # Debug Information
    print("Unique values in 'Compare' column:")
    print(df['Compare'].unique())
    print("Count of non-NaN values in '2023_x' and '2023_y':")
    print(f"2023_x: {df['2023_x'].notna().sum()}, 2023_y: {df['2023_y'].notna().sum()}")

    # Save the Result
    print(f"Saving processed file to: {output_file}")
    df.to_excel(output_file, index=False)

# Main Execution
if __name__ == "__main__":
    try:
        # Download the input file from MinIO
        download_from_minio(MINIO_BUCKET, INPUT_FILE_KEY, LOCAL_INPUT_FILE)

        # Process the financial data to create FIX
        process_financial_data(LOCAL_INPUT_FILE, LOCAL_OUTPUT_FILE)

        # Upload the result back to MinIO
        upload_to_minio(MINIO_BUCKET, MINIO_OUTPUT_KEY, LOCAL_OUTPUT_FILE)
        print(f"File successfully processed and uploaded to MinIO: {MINIO_OUTPUT_KEY}")
    except Exception as e:
        print(f"An error occurred: {e}")
