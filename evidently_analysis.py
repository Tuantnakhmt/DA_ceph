import pandas as pd
import numpy as np
import os
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
import boto3
from io import StringIO

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# # Load reference data from MinIO
# try:
#     print("Loading reference data from MinIO...")
#     response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/Financial_with_Sentiment.csv")
#     reference_data = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
#     print("Reference data loaded successfully.")
# except Exception as e:
#     print(f"Error loading reference data: {e}")
#     exit(1)
# Load Reference Data from MinIO
print("Loading reference data from MinIO...")
try:
    # response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/Financial_with_Sentiment.csv")
    #response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/dataset_sample.csv")
    response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/Financial_with_Sentiment_1.csv")
    reference_data = pd.read_csv(
        StringIO(response['Body'].read().decode('utf-8')),
        low_memory=False  # Disables type inference for memory optimization
    )
    # reference_data = pd.read_csv(
    #     StringIO(response['Body'].read().decode('utf-8')),
    #     low_memory=False  # Disables type inference for memory optimization
    # ).head(10)
    print("Reference data loaded successfully.")
except Exception as e:
    print(f"Error loading reference data from MinIO: {e}")
    exit(1)
    
# # Generate pseudo data
# print("Generating pseudo data...")
# np.random.seed(42)
# pseudo_data = reference_data.copy()
# for column in pseudo_data.select_dtypes(include=[np.number]).columns:
#     pseudo_data[column] = pseudo_data[column] * np.random.uniform(0.9, 1.1, size=len(pseudo_data))
# print("Pseudo data generated.")
# Generate pseudo data
print("Generating pseudo data...")

# # Preprocess: Remove columns with high NaN or zero ratios
# nan_threshold = 0.5  # Remove columns with more than 50% NaN values
# zero_threshold = 0.5  # Remove columns with more than 50% zero values

# # Calculate the NaN and zero ratios for each column
# nan_ratios = reference_data.isna().mean()
# zero_ratios = (reference_data == 0).mean()


# # Filter out columns exceeding the thresholds
# filtered_data_sample = reference_data.loc[:, (nan_ratios < nan_threshold) & (zero_ratios < zero_threshold)]
filtered_data_sample = reference_data.copy()
# Select numeric columns
numeric_cols_filtered = filtered_data_sample.select_dtypes(include=[np.number]).columns

# Create a pseudo-drifted version of the data
drifted_filtered_data = filtered_data_sample.copy()
for col in numeric_cols_filtered:
    drifted_filtered_data[col] = np.random.uniform(
        low=filtered_data_sample[col].min() * 1.5, 
        high=filtered_data_sample[col].max() * 2.0, 
        size=filtered_data_sample[col].shape
    )
drifted_filtered_data[numeric_cols_filtered] = drifted_filtered_data[numeric_cols_filtered].clip(lower=0)


# Save pseudo data to /tmp/ first
try:
    pseudo_csv_path = "/tmp/Financial_with_Sentiment_pseudo.csv"
    # pseudo_data.to_csv(pseudo_csv_path, index=False)
    drifted_filtered_data.to_csv(pseudo_csv_path, index=False)
    print(f"Pseudo data saved locally at {pseudo_csv_path}")

    # Upload from /tmp/ to MinIO
    with open(pseudo_csv_path, "rb") as pseudo_csv_file:
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key="data/Ingestion/Financial_with_Sentiment_pseudo.csv",
            Body=pseudo_csv_file
        )
    print("Pseudo data uploaded successfully to MinIO.")
except Exception as e:
    print(f"Error saving or uploading pseudo data: {e}")

# # Generate Evidently reports
# print("Running Evidently analysis...")
# report_drift = Report(metrics=[DataDriftPreset()])
# report_quality = Report(metrics=[DataQualityPreset()])

# report_drift.run(reference_data=reference_data, current_data=pseudo_data)
# report_quality.run(reference_data=reference_data, current_data=pseudo_data)

# Define the threshold for missing values
threshold = 0.5  # 70%

# Calculate the maximum number of non-null values allowed
max_non_null = len(reference_data) * (1 - threshold)

# # Drop columns with more than 70% NaN
# filtered_reference_data = reference_data.dropna(axis=1, thresh=max_non_null)
# # Drop columns with more than 70% NaN
# # filtered_pseudo_data = pseudo_data.dropna(axis=1, thresh=max_non_null)

# filtered_pseudo_data = drifted_filtered_data.dropna(axis=1, thresh=max_non_null)
filtered_reference_data = filtered_data_sample.copy()
filtered_pseudo_data = drifted_filtered_data.copy()
# # Define the threshold for missing or zero values
# threshold = 0.7  # 70%

# # Calculate the maximum number of valid (non-NaN and non-zero) values allowed
# max_invalid = len(filtered_reference_data) * threshold  # Number of NaN + zero values threshold

# # Custom function to count NaN + zero values
# def count_invalid_values(column):
#     return column.isna().sum() + (column == 0).sum()

# # Filter columns
# valid_columns = [col for col in filtered_reference_data.columns if count_invalid_values(filtered_reference_data[col]) <= max_invalid]

# # Keep only valid columns
# filtered_reference_data = filtered_reference_data[valid_columns]

# # Filter columns
# valid_columns_2 = [col for col in filtered_pseudo_data.columns if count_invalid_values(filtered_pseudo_data[col]) <= max_invalid]

# # Keep only valid columns
# filtered_pseudo_data = filtered_pseudo_data[valid_columns_2]

# # Run Evidently Analysis
# print("Running Evidently analysis...")
# report_drift = Report(metrics=[DataDriftPreset()])
# report_quality = Report(metrics=[DataQualityPreset()])

# report_drift.run(reference_data=filtered_reference_data, current_data=filtered_pseudo_data)
# report_quality.run(reference_data=filtered_reference_data, current_data=filtered_pseudo_data)

# # Save Evidently reports to /tmp/ first
# try:
#     drift_report_path = "/tmp/data_drift_report.html"
#     quality_report_path = "/tmp/data_quality_report.html"

#     # Save drift report locally
#     with open(drift_report_path, "wb") as drift_file:
#         report_drift.save_html(drift_file)
#     print(f"Data drift report saved locally at {drift_report_path}")

#     # Save quality report locally
#     with open(quality_report_path, "wb") as quality_file:
#         report_quality.save_html(quality_file)
#     print(f"Data quality report saved locally at {quality_report_path}")

#     # Upload drift report to MinIO
#     with open(drift_report_path, "rb") as drift_file:
#         s3.put_object(
#             Bucket=MINIO_BUCKET,
#             Key="data/Ingestion/data_drift_report.html",
#             Body=drift_file
#         )
#     print("Data drift report uploaded successfully to MinIO.")

#     # Upload quality report to MinIO
#     with open(quality_report_path, "rb") as quality_file:
#         s3.put_object(
#             Bucket=MINIO_BUCKET,
#             Key="data/Ingestion/data_quality_report.html",
#             Body=quality_file
#         )
#     print("Data quality report uploaded successfully to MinIO.")
# except Exception as e:
#     print(f"Error saving or uploading Evidently reports: {e}")

# print("Process completed.")

# Run Evidently Analysis

# Preprocess Data: Remove columns with zero variance or all NaN
def preprocess_data(data):
    # Drop columns with all NaN values
    data = data.dropna(axis=1, how='all')
    # Drop columns with zero variance
    data = data.loc[:, data.nunique() > 1]
    return data

# # Preprocess reference and pseudo data
# filtered_reference_data = preprocess_data(filtered_reference_data)
# filtered_pseudo_data = preprocess_data(filtered_pseudo_data)

# # Ensure columns match
# filtered_pseudo_data = filtered_pseudo_data[filtered_reference_data.columns]

print("Running Evidently analysis...")
report_drift = Report(metrics=[DataDriftPreset()])
report_quality = Report(metrics=[DataQualityPreset()])

# Run reports on reference and pseudo data
report_drift.run(reference_data=filtered_reference_data, current_data=filtered_pseudo_data)
report_quality.run(reference_data=filtered_reference_data, current_data=filtered_pseudo_data)

# # Check for drift immediately
# drift_report = Report(metrics=[DataDriftPreset()])
# drift_report.run(reference_data=filtered_data_sample, current_data=drifted_filtered_data)
# drift_detected = report_drift.as_dict()["metrics"][0]["result"]["dataset_drift"]

# print(f"Drift detected: {drift_detected}")

# Save Evidently reports to /tmp/ first
try:
    drift_report_path = "/tmp/data_drift_report_main.html"
    quality_report_path = "/tmp/data_quality_report_main.html"
    # drift_report_path = "/tmp/data_drift_report.html"
    # quality_report_path = "/tmp/data_quality_report.html"

    # Save drift report locally
    report_drift.save_html(drift_report_path)
    print(f"Data drift report saved locally at {drift_report_path}")

    # Save quality report locally
    report_quality.save_html(quality_report_path)
    print(f"Data quality report saved locally at {quality_report_path}")

    # Upload drift report to MinIO
    with open(drift_report_path, "rb") as drift_file:
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key="data/Ingestion/data_drift_report_main.html",
            Body=drift_file
        )
    print("Data drift report uploaded successfully to MinIO.")

    # Upload quality report to MinIO
    try:
        # Upload quality report to MinIO
        with open(quality_report_path, "rb") as quality_file:
            s3.put_object(
                Bucket=MINIO_BUCKET,
                Key="data/Ingestion/data_quality_report_main.html",
                Body=quality_file
            )
        print("Data quality report uploaded successfully to MinIO.")
    except Exception as e:
        print(f"Error uploading Data Quality Report to MinIO: {e}")
except Exception as e:
    print(f"Error saving or uploading Evidently reports: {e}")

print("Process completed.")
import json
SAVE_PATH = "data/Ingestion/drift_status.json"
def save_drift_status_to_minio(drift_detected):
    drift_status = {"drift_detected": drift_detected}
    json_data = json.dumps(drift_status).encode('utf-8')
    
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=SAVE_PATH,
        Body=json_data,
        ContentType='application/json'
    )
    print(f"Trang thai drift da duoc luu: {SAVE_PATH}")

drift_detected = True  
save_drift_status_to_minio(drift_detected)