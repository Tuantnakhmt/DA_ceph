# import boto3
# import pandas as pd
# import os
# import json
# from io import BytesIO
# from botocore.exceptions import ClientError

# # Initialize MinIO client
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# minio_client = boto3.client(
#     "s3",
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# # Download file from MinIO
# def download_file_from_minio(bucket, file_key):
#     try:
#         response = minio_client.get_object(Bucket=bucket, Key=file_key)
#         return pd.read_csv(BytesIO(response["Body"].read()))
#     except ClientError as e:
#         print(f"Error downloading {file_key} from MinIO: {e}")
#         return None

# # Check if a file exists in MinIO
# def check_file_exists(bucket, file_key):
#     try:
#         minio_client.head_object(Bucket=bucket, Key=file_key)
#         return True
#     except ClientError:
#         return False

# # Process symbol files
# def process_close_files(symbol):
#     print(f"Processing {symbol} for Close...")
#     file_key = f"data/Ingestion/Close/CafeF/{symbol}/{symbol}.csv"
#     if not check_file_exists(MINIO_BUCKET, file_key):
#         print(f"Error processing {symbol}: {file_key} not found in MinIO.")
#         return

#     data = download_file_from_minio(MINIO_BUCKET, file_key)
#     if data is not None:
#         print(f"Successfully processed Close data for {symbol}.")
#     else:
#         print(f"Failed to process Close data for {symbol}.")


# def process_financial_files(symbol, field, type_time):
#     print(f"Processing {symbol} for Financial ({field})...")
#     file_key = f"data/Ingestion/Financial/CafeF/Year/{field}/{symbol}.json"
#     if not check_file_exists(MINIO_BUCKET, file_key):
#         print(f"Error processing {symbol}: {file_key} not found in MinIO.")
#         return

#     try:
#         response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_key)
#         data = json.loads(response["Body"].read())
#         print(f"Successfully processed Financial ({field}) data for {symbol}.")
#     except Exception as e:
#         print(f"Failed to process Financial ({field}) data for {symbol}: {e}")

        

# if __name__ == "__main__":
#     # Download the List_company.csv file
#     list_company_key = "data/Ingestion/Close/CafeF/List_company.csv"
#     list_company_df = download_file_from_minio(MINIO_BUCKET, list_company_key)

#     if list_company_df is not None:
#         symbols = list_company_df["Mã CK"].tolist()

#         # Process Close files
#         for symbol in symbols:
#             process_close_files(symbol)

#         # Process Financial files
#         fields = ["BalanceSheet", "IncomeStatement"]
#         for symbol in symbols:
#             for field in fields:
#                 process_financial_files(symbol, field, "Year")
#     else:
#         print("Failed to download or read List_company.csv")
import boto3
import json
import pandas as pd
import os
from botocore.exceptions import ClientError

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize MinIO client
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def check_file_exists(bucket, file_key):
    """Check if a file exists in MinIO"""
    try:
        minio_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise

def process_close_files(symbol):
    """Process close data for a symbol"""
    print(f"Processing {symbol} for Close...")
    file_key = f"data/Ingestion/Close/CafeF/{symbol}/{symbol}.csv"
    if not check_file_exists(MINIO_BUCKET, file_key):
        print(f"Error processing {symbol}: {file_key} not found in MinIO.")
        return

    try:
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_key)
        data = pd.read_csv(response["Body"])
        print(f"Successfully processed Close data for {symbol}.")
    except Exception as e:
        print(f"Failed to process Close data for {symbol}: {e}")


def process_financial_files(symbol, field, type_time):
    """Process financial data for a symbol"""
    print(f"Processing {symbol} for Financial ({field})...")
    file_key = f"data/Ingestion/Financial/CafeF/Year/{field}/{symbol}.json"
    if not check_file_exists(MINIO_BUCKET, file_key):
        print(f"Error processing {symbol}: {file_key} not found in MinIO.")
        return

    try:
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_key)
        data = json.loads(response["Body"].read())
        print(f"Successfully processed Financial ({field}) data for {symbol}.")
    except Exception as e:
        print(f"Failed to process Financial ({field}) data for {symbol}: {e}")


def process_vietstock_files(symbol, field):
    """Process VietStock data for a symbol"""
    print(f"Processing {symbol} for VietStock ({field})...")
    file_key = f"data/Ingestion/Financial/VietStock/Year/{field}/{symbol}.csv"
    if not check_file_exists(MINIO_BUCKET, file_key):
        print(f"Error processing {symbol}: {file_key} not found in MinIO.")
        return

    try:
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_key)
        data = pd.read_csv(response["Body"])
        print(f"Successfully processed VietStock ({field}) data for {symbol}.")
    except Exception as e:
        print(f"Failed to process VietStock ({field}) data for {symbol}: {e}")


def main():
    # Download List_company.csv from MinIO
    list_company_key = "data/Ingestion/Close/CafeF/List_company.csv"
    local_list_company_path = "/tmp/List_company.csv"
    try:
        minio_client.download_file(MINIO_BUCKET, list_company_key, local_list_company_path)
        print(f"Downloaded {list_company_key} from MinIO to {local_list_company_path}")
    except Exception as e:
        print(f"Failed to download {list_company_key} from MinIO: {e}")
        return

    # Read symbols from List_company.csv
    try:
        symbols = pd.read_csv(local_list_company_path)["Mã CK"].tolist()
    except Exception as e:
        print(f"Failed to read {local_list_company_path}: {e}")
        return

    # Process files for each symbol
    for symbol in symbols:
        process_close_files(symbol)
        process_financial_files(symbol, "BalanceSheet", "Year")
        process_financial_files(symbol, "IncomeStatement", "Year")
        process_vietstock_files(symbol, "BalanceSheet")
        process_vietstock_files(symbol, "IncomeStatement")

if __name__ == "__main__":
    main()

