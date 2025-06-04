import logging 
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import for PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score
from scipy import stats
import os
import boto3
# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def download_from_minio(bucket, key, local_path):
    with open(local_path, "wb") as f:
        s3.download_fileobj(bucket, key, f)
    logging.info(f"Downloaded {key} from MinIO to {local_path}")

def upload_to_minio(local_path, bucket, key):
    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, bucket, key)
    logging.info(f"Uploaded {local_path} to MinIO at {key}")

def clean_column(column):
    return pd.to_numeric(column, errors='coerce')

def train_xgboost_model():
    logging.info("Starting XGBoost model training...")
    
    # input_file = "/tmp/Financial_with_Sentiment.csv"
    # download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", input_file)
    input_file = "/tmp/Financial_with_Sentiment_1.csv"
    download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_1.csv", input_file)

    df = pd.read_csv(input_file)
    
    for column in df.select_dtypes(include=['object']).columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    df["ValueTrading"] = pd.to_numeric(df["ValueTrading"], errors="coerce").fillna(0)

    threshold = 0.7
    max_non_null = len(df) * (1 - threshold)
    df = df.dropna(axis=1, thresh=max_non_null)
    df.fillna(0, inplace=True)

    z_scores = np.abs(stats.zscore(df["ValueTrading"]))
    df = df[(z_scores < 3)]

    corr_matrix = df.corr()
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != "ValueTrading"]
    df = df.drop(columns=to_drop)
    
    target = "ValueTrading"
    X = df.drop(columns=[target])
    y = df[target]
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    
    xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
    xgb_model.fit(X_train, y_train)
    
    y_pred = xgb_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    # logging.info(f"Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
    
    output_dir = "/tmp/output"
    os.makedirs(output_dir, exist_ok=True)
    
    predictions_path = os.path.join(output_dir, "predictions_1.csv")
    predictions_df = pd.DataFrame({
        "True Values": y_test,
        "Predicted Values": y_pred,
        "Residuals": y_test - y_pred
    })
    predictions_df.to_csv(predictions_path, index=False)
    upload_to_minio(predictions_path, MINIO_BUCKET, "data/Ingestion/predictions_1.csv")
    
    metrics_path = os.path.join(output_dir, "metrics_1.txt")
    with open(metrics_path, "w") as f:
        f.write(f"Mean Squared Error (MSE): {mse:.2f}\n")
        f.write(f"R-squared (R2): {r2:.2f}\n")
    upload_to_minio(metrics_path, MINIO_BUCKET, "data/Ingestion/metrics_1.txt")
    
    feature_importance_path = os.path.join(output_dir, "feature_importance_1.csv")
    feature_importance = pd.DataFrame({
        "Feature": df.drop(columns=[target]).columns,
        "Importance": xgb_model.feature_importances_
    }).sort_values(by="Importance", ascending=False)
    feature_importance.to_csv(feature_importance_path, index=False)
    upload_to_minio(feature_importance_path, MINIO_BUCKET, "data/Ingestion/feature_importance_1.csv")

    # logging.info("Training process completed successfully.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

current_date = datetime.now()

# Updated DAG definition with `schedule`
dag = DAG(
    dag_id="train_xgboost_pipeline",
    default_args=default_args,
    description="Train an XGBoost model and save outputs",
    schedule="@hourly",  # Use `schedule` instead of `schedule_interval`
    start_date=current_date,
    catchup=False,
)

train_task = PythonOperator(
    task_id="train_xgboost_model",
    python_callable=train_xgboost_model,
    dag=dag,
)