import os
import pandas as pd
from io import BytesIO
import boto3
from datetime import datetime

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize Boto3 MinIO client
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# MinIO Paths
MINIO_SYMBOL_FILE = "data/Ingestion/Close/CafeF/List_company.csv"
MINIO_CLOSE_PATH = "data/Ingestion/Close/CafeF"
F1_PATH = f"{MINIO_CLOSE_PATH}/F1"

# Helper function to check if a file exists in MinIO
def check_file_exists(bucket, key):
    try:
        minio_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

# Helper function to transform date format
def convert_time(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error converting time for {date_str}: {e}")
        return None

# Helper function to download List_company.csv and fetch symbols
def get_symbols_from_minio():
    try:
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=MINIO_SYMBOL_FILE)
        df = pd.read_csv(BytesIO(response['Body'].read()))
        return df["Mã CK"].tolist()  # Adjust column name if needed
    except Exception as e:
        print(f"Error fetching symbols from MinIO: {e}")
        return []

# Helper function to process close data for a symbol
def process_close_data(symbol):
    file_key = f"{MINIO_CLOSE_PATH}/{symbol}/{symbol}.csv"
    output_key = f"{F1_PATH}/{symbol}.csv"

    if not check_file_exists(MINIO_BUCKET, file_key):
        print(f"Source file not found for {symbol}: {file_key}")
        return

    try:
        # Read the close data from MinIO
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_key)
        df = pd.read_csv(BytesIO(response['Body'].read()))

        # Transform the data
        df = df[["Ngày", "Giá Đóng cửa", "GD Khối lượng"]]
        df = df.rename(columns={
            "Ngày": "Time",
            "Giá Đóng cửa": "ValueTrading",
            "GD Khối lượng": "VolumeTrading"
        })
        df["Time"] = df["Time"].apply(convert_time)
        df = df.sort_values(by="Time").reset_index(drop=True)

        # Save the transformed data back to MinIO
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        minio_client.put_object(
            Bucket=MINIO_BUCKET, Key=output_key, Body=csv_buffer.getvalue(), ContentType="text/csv"
        )
        print(f"Successfully processed and saved for {symbol}: {output_key}")
    except Exception as e:
        print(f"Failed to process {symbol}: {e}")

# Main execution
if __name__ == "__main__":
    print("Fetching symbols from MinIO...")
    symbols = get_symbols_from_minio()

    if not symbols:
        print("No symbols found. Exiting...")
    else:
        print(f"Processing close data for symbols: {symbols}")
        for symbol in symbols:
            process_close_data(symbol)
