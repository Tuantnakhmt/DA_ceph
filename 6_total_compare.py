
import pandas as pd
import sys
from io import BytesIO
import boto3
import os 
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


input_path_template = "data/Ingestion/Compare/Financial/Year/{symbol}.csv"
output_path = "data/Ingestion/Compare/Financial_Year_pipeline.xlsx"

def get_symbols_from_minio():
    try:
        local_file_path = "List_company.csv"
        minio_folder_path = "data/Ingestion/Close/CafeF"
        minio_key = f"{minio_folder_path}/{local_file_path}"
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=minio_key)
        with open(local_file_path, "wb") as file_data:
            file_data.write(response["Body"].read())
        company_list = pd.read_csv(local_file_path)
        # company_list = pd.read_csv(local_file_path).head(5)
        company_codes = company_list["Mã CK"].tolist()
        return company_codes
    except Exception as e:
        # print(f"Error loading company list from MinIO: {e}")
        raise

# Function to fetch comparison result from MinIO
def get_result_compare(symbol, input_path_template):
    try:
        input_key = input_path_template.format(symbol=symbol)
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=input_key)
        df_current = pd.read_csv(response["Body"])
        df_current["Symbol"] = symbol
        return df_current
    except Exception as e:
        print(f"Error reading comparison result for symbol {symbol}: {e}")
        return pd.DataFrame()

def generate_financial_year_pipeline():
    dict_compare = {"Financial_Year": pd.DataFrame()}
    SYMBOL = get_symbols_from_minio()

    for symbol in SYMBOL:
        try:
            df_current = get_result_compare(symbol, input_path_template)
            dict_compare["Financial_Year"] = pd.concat([dict_compare["Financial_Year"], df_current], ignore_index=True)
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")
            continue

    # Save combined result to MinIO
    try:
        output_buffer = BytesIO()
        dict_compare["Financial_Year"].to_excel(output_buffer, index=False)
        output_buffer.seek(0)
        minio_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=output_path,
            Body=output_buffer,
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        print(f"Combined result saved to MinIO at {output_path}")
    except Exception as e:
        print(f"Error saving combined result to MinIO: {e}")

# Run the process
generate_financial_year_pipeline()
