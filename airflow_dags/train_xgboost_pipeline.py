# import logging
# import boto3
# import os
# import pandas as pd
# import numpy as np
# from datetime import datetime
# from sklearn.model_selection import train_test_split
# from sklearn.preprocessing import StandardScaler
# from xgboost import XGBRegressor
# from sklearn.metrics import mean_squared_error, r2_score
# from scipy import stats

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# # Initialize MinIO client
# s3 = boto3.client(
#     "s3",
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# def download_from_minio(bucket, key, local_path):
#     with open(local_path, "wb") as f:
#         s3.download_fileobj(bucket, key, f)
#     logging.info(f"Downloaded {key} from MinIO to {local_path}")

# def upload_to_minio(local_path, bucket, key):
#     with open(local_path, "rb") as f:
#         s3.upload_fileobj(f, bucket, key)
#     logging.info(f"Uploaded {local_path} to MinIO at {key}")

# def train_xgboost_model():
#     logging.info("Starting XGBoost model training...")
    
#     # Download the input file
#     input_file = "/tmp/Financial_with_Sentiment.csv"
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", input_file)
    
#     # Load data
#     df = pd.read_csv(input_file)
    
#     # Data Cleaning
#     for column in df.select_dtypes(include=['object']).columns:
#         df[column] = pd.to_numeric(df[column], errors='coerce')
    
#     df["ValueTrading"] = pd.to_numeric(df["ValueTrading"], errors="coerce").fillna(0)

#     threshold = 0.7
#     max_non_null = len(df) * (1 - threshold)
#     df = df.dropna(axis=1, thresh=max_non_null)
#     df.fillna(0, inplace=True)

#     z_scores = np.abs(stats.zscore(df["ValueTrading"]))
#     df = df[(z_scores < 3)]

#     corr_matrix = df.corr()
#     upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
#     to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != "ValueTrading"]
#     df = df.drop(columns=to_drop)
    
#     target = "ValueTrading"
#     X = df.drop(columns=[target])
#     y = df[target]
    
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)
    
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    
#     xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
#     xgb_model.fit(X_train, y_train)
    
#     y_pred = xgb_model.predict(X_test)
#     mse = mean_squared_error(y_test, y_pred)
#     r2 = r2_score(y_test, y_pred)
#     logging.info(f"Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
    
#     # Save outputs
#     output_dir = "/tmp/output"
#     os.makedirs(output_dir, exist_ok=True)
    
#     predictions_path = os.path.join(output_dir, "predictions.csv")
#     predictions_df = pd.DataFrame({
#         "True Values": y_test,
#         "Predicted Values": y_pred,
#         "Residuals": y_test - y_pred
#     })
#     predictions_df.to_csv(predictions_path, index=False)
#     upload_to_minio(predictions_path, MINIO_BUCKET, "data/Ingestion/predictions.csv")
    
#     metrics_path = os.path.join(output_dir, "metrics.txt")
#     with open(metrics_path, "w") as f:
#         f.write(f"Mean Squared Error (MSE): {mse:.2f}\n")
#         f.write(f"R-squared (R2): {r2:.2f}\n")
#     upload_to_minio(metrics_path, MINIO_BUCKET, "data/Ingestion/metrics.txt")
    
#     feature_importance_path = os.path.join(output_dir, "feature_importance.csv")
#     feature_importance = pd.DataFrame({
#         "Feature": df.drop(columns=[target]).columns,
#         "Importance": xgb_model.feature_importances_
#     }).sort_values(by="Importance", ascending=False)
#     feature_importance.to_csv(feature_importance_path, index=False)
#     upload_to_minio(feature_importance_path, MINIO_BUCKET, "data/Ingestion/feature_importance.csv")

#     logging.info("Training process completed successfully.")

# # Define DAG
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
# }

# dag = DAG(
#     "train_xgboost_pipeline",
#     default_args=default_args,
#     description="Train an XGBoost model and save outputs",
#     schedule_interval=None,
#     start_date=datetime(2024, 12, 21),
#     catchup=False,
# )

# train_task = PythonOperator(
#     task_id="train_xgboost_model",
#     python_callable=train_xgboost_model,
#     dag=dag,
# )


# import logging 
# from airflow import DAG
# from airflow.operators.python import PythonOperator  # Updated import for PythonOperator
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.preprocessing import StandardScaler
# from xgboost import XGBRegressor
# from sklearn.metrics import mean_squared_error, r2_score
# from scipy import stats
# import os
# import boto3
# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# # Initialize MinIO client
# s3 = boto3.client(
#     "s3",
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# def download_from_minio(bucket, key, local_path):
#     with open(local_path, "wb") as f:
#         s3.download_fileobj(bucket, key, f)
#     logging.info(f"Downloaded {key} from MinIO to {local_path}")

# def upload_to_minio(local_path, bucket, key):
#     with open(local_path, "rb") as f:
#         s3.upload_fileobj(f, bucket, key)
#     logging.info(f"Uploaded {local_path} to MinIO at {key}")

# def clean_column(column):
#     return pd.to_numeric(column, errors='coerce')

# def train_xgboost_model():
#     logging.info("Starting XGBoost model training...")
    
#     # Download the input file
#     input_file = "/tmp/Financial_with_Sentiment.csv"
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", input_file)
    
#     # Load data
#     df = pd.read_csv(input_file)
    
#     # Data Cleaning
#     for column in df.select_dtypes(include=['object']).columns:
#         df[column] = pd.to_numeric(df[column], errors='coerce')
    
#     df["ValueTrading"] = pd.to_numeric(df["ValueTrading"], errors="coerce").fillna(0)

#     threshold = 0.7
#     max_non_null = len(df) * (1 - threshold)
#     df = df.dropna(axis=1, thresh=max_non_null)
#     df.fillna(0, inplace=True)

#     z_scores = np.abs(stats.zscore(df["ValueTrading"]))
#     df = df[(z_scores < 3)]

#     corr_matrix = df.corr()
#     upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
#     to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != "ValueTrading"]
#     df = df.drop(columns=to_drop)
    
#     target = "ValueTrading"
#     X = df.drop(columns=[target])
#     y = df[target]
    
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)
    
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    
#     xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
#     xgb_model.fit(X_train, y_train)
    
#     y_pred = xgb_model.predict(X_test)
#     mse = mean_squared_error(y_test, y_pred)
#     r2 = r2_score(y_test, y_pred)
#     logging.info(f"Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
    
#     # Save outputs
#     output_dir = "/tmp/output"
#     os.makedirs(output_dir, exist_ok=True)
    
#     predictions_path = os.path.join(output_dir, "predictions.csv")
#     predictions_df = pd.DataFrame({
#         "True Values": y_test,
#         "Predicted Values": y_pred,
#         "Residuals": y_test - y_pred
#     })
#     predictions_df.to_csv(predictions_path, index=False)
#     upload_to_minio(predictions_path, MINIO_BUCKET, "data/Ingestion/predictions.csv")
    
#     metrics_path = os.path.join(output_dir, "metrics.txt")
#     with open(metrics_path, "w") as f:
#         f.write(f"Mean Squared Error (MSE): {mse:.2f}\n")
#         f.write(f"R-squared (R2): {r2:.2f}\n")
#     upload_to_minio(metrics_path, MINIO_BUCKET, "data/Ingestion/metrics.txt")
    
#     feature_importance_path = os.path.join(output_dir, "feature_importance.csv")
#     feature_importance = pd.DataFrame({
#         "Feature": df.drop(columns=[target]).columns,
#         "Importance": xgb_model.feature_importances_
#     }).sort_values(by="Importance", ascending=False)
#     feature_importance.to_csv(feature_importance_path, index=False)
#     upload_to_minio(feature_importance_path, MINIO_BUCKET, "data/Ingestion/feature_importance.csv")

#     logging.info("Training process completed successfully.")

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
# }

# current_date = datetime.now()

# # Updated DAG definition with `schedule`
# dag = DAG(
#     dag_id="train_xgboost_pipeline",
#     default_args=default_args,
#     description="Train an XGBoost model and save outputs",
#     schedule="@hourly",  # Use `schedule` instead of `schedule_interval`
#     start_date=current_date,
#     catchup=False,
# )

# train_task = PythonOperator(
#     task_id="train_xgboost_model",
#     python_callable=train_xgboost_model,
#     dag=dag,
# )

#--------------
# import logging
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.preprocessing import StandardScaler
# from xgboost import XGBRegressor
# from sklearn.metrics import mean_squared_error, r2_score
# from scipy import stats
# import os
# import boto3

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# # Initialize MinIO client
# s3 = boto3.client(
#     "s3",
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# def download_from_minio(bucket, key, local_path):
#     with open(local_path, "wb") as f:
#         s3.download_fileobj(bucket, key, f)
#     logging.info(f"Downloaded {key} from MinIO to {local_path}")

# def upload_to_minio(local_path, bucket, key):
#     with open(local_path, "rb") as f:
#         s3.upload_fileobj(f, bucket, key)
#     logging.info(f"Uploaded {local_path} to MinIO at {key}")

# def train_xgboost_model():
#     logging.info("Starting XGBoost model training...")
    
#     # Download the input file
#     input_file = "/tmp/Financial_with_Sentiment.csv"
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", input_file)
    
#     # Load data
#     df = pd.read_csv(input_file)
    
#     # Data Cleaning
#     for column in df.select_dtypes(include=['object']).columns:
#         df[column] = pd.to_numeric(df[column], errors='coerce')
    
#     df["ValueTrading"] = pd.to_numeric(df["ValueTrading"], errors="coerce").fillna(0)

#     threshold = 0.7
#     max_non_null = len(df) * (1 - threshold)
#     df = df.dropna(axis=1, thresh=max_non_null)
#     df.fillna(0, inplace=True)

#     z_scores = np.abs(stats.zscore(df["ValueTrading"]))
#     df = df[(z_scores < 3)]

#     corr_matrix = df.corr()
#     upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
#     to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != "ValueTrading"]
#     df = df.drop(columns=to_drop)
    
#     target = "ValueTrading"
#     X = df.drop(columns=[target])
#     y = df[target]
    
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)
    
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    
#     xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
#     xgb_model.fit(X_train, y_train)
    
#     y_pred = xgb_model.predict(X_test)
#     mse = mean_squared_error(y_test, y_pred)
#     r2 = r2_score(y_test, y_pred)
#     logging.info(f"Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")

#     # Save outputs
#     output_dir = "/tmp/output"
#     os.makedirs(output_dir, exist_ok=True)
    
#     predictions_path = os.path.join(output_dir, "predictions.csv")
#     predictions_df = pd.DataFrame({
#         "True Values": y_test,
#         "Predicted Values": y_pred,
#         "Residuals": y_test - y_pred
#     })
#     predictions_df.to_csv(predictions_path, index=False)
#     upload_to_minio(predictions_path, MINIO_BUCKET, "data/Ingestion/predictions.csv")
    
#     metrics_path = os.path.join(output_dir, "metrics.txt")
#     with open(metrics_path, "w") as f:
#         f.write(f"Mean Squared Error (MSE): {mse:.2f}\n")
#         f.write(f"R-squared (R2): {r2:.2f}\n")
#     upload_to_minio(metrics_path, MINIO_BUCKET, "data/Ingestion/metrics.txt")
    
#     logging.info("Training process completed successfully.")

# def preprocess_data(df, target_column, reference_features=None):
#     """
#     Preprocesses the input data by cleaning, handling missing values,
#     scaling, and ensuring consistent features.
#     """
#     # Convert all object columns to numeric
#     for column in df.select_dtypes(include=['object']).columns:
#         df[column] = pd.to_numeric(df[column], errors='coerce')
    
#     # Ensure target column is numeric
#     if target_column in df.columns:
#         df[target_column] = pd.to_numeric(df[target_column], errors="coerce").fillna(0)

#     # Drop columns with high missing values
#     threshold = 0.7
#     max_non_null = len(df) * (1 - threshold)
#     df = df.dropna(axis=1, thresh=max_non_null)
#     df.fillna(0, inplace=True)

#     # Remove outliers from the target column (only if target exists)
#     if target_column in df.columns:
#         z_scores = np.abs(stats.zscore(df[target_column]))
#         df = df[(z_scores < 3)]

#     # Remove highly correlated features if reference_features is None
#     if reference_features is None:
#         corr_matrix = df.corr()
#         upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
#         to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != target_column]
#         df = df.drop(columns=to_drop)

#     # Align with reference features if provided
#     if reference_features is not None:
#         # Ensure target column is not dropped
#         all_columns = list(reference_features) + [target_column]
#         df = df.reindex(columns=all_columns, fill_value=0)

#     # Separate features and target
#     X = df.drop(columns=[target_column], errors="ignore")
#     y = df[target_column] if target_column in df.columns else None

#     # Scale the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     return X_scaled, y, X.columns



# def check_drift_and_retrain():
#     logging.info("Starting drift detection and retraining if necessary...")

#     # Use reference and current data for testing
#     reference_file = "/tmp/Financial_with_Sentiment.csv"
#     current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"

#     # Download datasets from MinIO
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", reference_file)
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

#     # Load datasets
#     reference_df = pd.read_csv(reference_file)
#     current_df = pd.read_csv(current_file)

#     # Preprocess reference data to extract feature columns
#     target = "ValueTrading"
#     reference_X_scaled, reference_y, feature_columns = preprocess_data(reference_df, target)

#     # Preprocess current data with the same feature columns
#     current_X_scaled, current_y, _ = preprocess_data(current_df, target, reference_features=feature_columns)

#     # Force drift detection to "True" for testing
#     drift_detected = True
#     logging.info("Drift detection result: Forced to True for testing.")

#     # Retrain if drift is detected
#     if drift_detected:
#         logging.info("Drift detected! Retraining the model...")
#         X_train, X_test, y_train, y_test = train_test_split(
#             reference_X_scaled, reference_y, test_size=0.2, random_state=42
#         )
#         xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
#         xgb_model.fit(X_train, y_train)

#         # Evaluate new model on the current dataset
#         y_pred = xgb_model.predict(current_X_scaled)
#         mse = mean_squared_error(current_y, y_pred)
#         r2 = r2_score(current_y, y_pred)
#         logging.info(f"Retrained Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
#         print(f"Retrained Model Metrics:\nMSE: {mse:.2f}\nR2: {r2:.2f}")
#     else:
#         logging.info("No drift detected. Model retraining skipped.")

# # from evidently import ColumnMapping
# # from evidently.report import Report
# # from evidently.metric_preset import DataDriftPreset


# # def preprocess_data(df, target_column, reference_features=None):
# #     """
# #     Preprocesses the input data by cleaning, handling missing values,
# #     scaling, and ensuring consistent features.
# #     """
# #     # Convert all object columns to numeric
# #     for column in df.select_dtypes(include=['object']).columns:
# #         df[column] = pd.to_numeric(df[column], errors='coerce')
    
# #     # Ensure target column is numeric
# #     if target_column in df.columns:
# #         df[target_column] = pd.to_numeric(df[target_column], errors="coerce").fillna(0)

# #     # Drop columns with high missing values
# #     threshold = 0.7
# #     max_non_null = len(df) * (1 - threshold)
# #     df = df.dropna(axis=1, thresh=max_non_null)
# #     df.fillna(0, inplace=True)

# #     # Remove outliers from the target column (only if target exists)
# #     if target_column in df.columns:
# #         z_scores = np.abs(stats.zscore(df[target_column]))
# #         df = df[(z_scores < 3)]

# #     # Remove highly correlated features if reference_features is None
# #     if reference_features is None:
# #         corr_matrix = df.corr()
# #         upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
# #         to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != target_column]
# #         df = df.drop(columns=to_drop)

# #     # Align with reference features if provided
# #     if reference_features is not None:
# #         # Ensure target column is not dropped
# #         all_columns = list(reference_features) + [target_column]
# #         df = df.reindex(columns=all_columns, fill_value=0)

# #     # Separate features and target
# #     X = df.drop(columns=[target_column], errors="ignore")
# #     y = df[target_column] if target_column in df.columns else None

# #     # Scale the features
# #     scaler = StandardScaler()
# #     X_scaled = scaler.fit_transform(X)

# #     return X_scaled, y, X.columns

# # def check_drift_and_retrain():
# #     logging.info("Starting drift detection and retraining if necessary...")

# #     # Use reference and current data
# #     reference_file = "/tmp/Financial_with_Sentiment.csv"
# #     current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"

# #     # Download datasets from MinIO
# #     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", reference_file)
# #     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

# #     # Load datasets
# #     reference_df = pd.read_csv(reference_file)
# #     current_df = pd.read_csv(current_file)

# #     # Preprocess reference data to extract feature columns
# #     target = "ValueTrading"
# #     reference_X_scaled, reference_y, feature_columns = preprocess_data(reference_df, target)

# #     # Preprocess current data with the same feature columns
# #     current_X_scaled, current_y, _ = preprocess_data(current_df, target, reference_features=feature_columns)

# #     # Use Evidently for drift detection
# #     reference_data = pd.DataFrame(reference_X_scaled, columns=feature_columns)
# #     current_data = pd.DataFrame(current_X_scaled, columns=feature_columns)
# #     column_mapping = ColumnMapping(target=None)

# #     drift_report = Report(metrics=[DataDriftPreset()])
# #     drift_report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)
# #     drift_results = drift_report.as_dict()
# #     drift_detected = drift_results['metrics'][0]['result']['dataset_drift']

# #     logging.info(f"Drift detection result: {drift_detected}")

# #     # Retrain if drift is detected
# #     if drift_detected:
# #         logging.info("Drift detected! Retraining the model...")
# #         X_train, X_test, y_train, y_test = train_test_split(
# #             reference_X_scaled, reference_y, test_size=0.2, random_state=42
# #         )
# #         xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
# #         xgb_model.fit(X_train, y_train)

# #         # Evaluate new model on the current dataset
# #         y_pred = xgb_model.predict(current_X_scaled)
# #         mse = mean_squared_error(current_y, y_pred)
# #         r2 = r2_score(current_y, y_pred)
# #         logging.info(f"Retrained Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
# #         print(f"Retrained Model Metrics:\nMSE: {mse:.2f}\nR2: {r2:.2f}")
# #     else:
# #         logging.info("No drift detected. Model retraining skipped.")



# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
# }

# current_date = datetime.now()

# dag = DAG(
#     dag_id="train_and_retrain_xgboost_pipeline",
#     default_args=default_args,
#     description="Train and retrain an XGBoost model and save outputs",
#     schedule="@hourly",
#     start_date=current_date,
#     catchup=False,
# )

# train_task = PythonOperator(
#     task_id="train_xgboost_model",
#     python_callable=train_xgboost_model,
#     dag=dag,
# )

# retrain_task = PythonOperator(
#     task_id="check_drift_and_retrain",
#     python_callable=check_drift_and_retrain,
#     dag=dag,
# )

# train_task >> retrain_task
#---------------------------------

import subprocess
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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
import pickle

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

def install_package(package_name):
    """Dynamically install a Python package."""
    subprocess.check_call(["pip", "install", package_name])

# Ensure joblib is installed
try:
    import joblib
except ImportError:
    install_package("joblib")
    import joblib

# try:
#     import evidently
# except ImportError:
#     install_package("evidently==0.4.16")
#     install_package("numpy==1.23.5")
#     from evidently.report import Report
#     from evidently.metric_preset import DataDriftPreset


def download_from_minio(bucket, key, local_path):
    with open(local_path, "wb") as f:
        s3.download_fileobj(bucket, key, f)
    logging.info(f"Downloaded {key} from MinIO to {local_path}")

def upload_to_minio(local_path, bucket, key):
    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, bucket, key)
    logging.info(f"Uploaded {local_path} to MinIO at {key}")

def preprocess_data(df, target_column, reference_features=None):
    """
    Preprocesses the input data by cleaning, handling missing values,
    scaling, and ensuring consistent features.
    """
    for column in df.select_dtypes(include=['object']).columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    if target_column in df.columns:
        df[target_column] = pd.to_numeric(df[target_column], errors="coerce").fillna(0)

    threshold = 0.7
    max_non_null = len(df) * (1 - threshold)
    df = df.dropna(axis=1, thresh=max_non_null)
    df.fillna(0, inplace=True)

    if target_column in df.columns:
        z_scores = np.abs(stats.zscore(df[target_column]))
        df = df[(z_scores < 3)]

    if reference_features is None:
        corr_matrix = df.corr()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper.columns if any(upper[column] > 0.85) and column != target_column]
        df = df.drop(columns=to_drop)

    if reference_features is not None:
        all_columns = list(reference_features) + [target_column]
        df = df.reindex(columns=all_columns, fill_value=0)

    X = df.drop(columns=[target_column], errors="ignore")
    y = df[target_column] if target_column in df.columns else None
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, y, X.columns

import json
SAVE_PATH = "data/Ingestion/drift_status.json"
def get_drift_status_from_minio():
    """
    Retrieve drift detection status from MinIO
    """
    response = s3.get_object(Bucket=MINIO_BUCKET, Key=SAVE_PATH)
    drift_status = json.loads(response['Body'].read().decode('utf-8'))
    return drift_status["drift_detected"]

def check_drift():
    logging.info("Checking for drift...")

    # # Download reference and current data from MinIO
    # reference_file = "/tmp/Financial_with_Sentiment.csv"
    # current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"
    # download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", reference_file)
    # download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

    # # Load datasets
    # reference_df = pd.read_csv(reference_file)
    # current_df = pd.read_csv(current_file)

    # # Preprocess reference and current data
    # target = "ValueTrading"
    # _, _, feature_columns = preprocess_data(reference_df, target)
    # _, _, _ = preprocess_data(current_df, target, reference_features=feature_columns)

    drift_detected = get_drift_status_from_minio()
    logging.info(f"Drift detected: {drift_detected}")

    # return "retrain_model" if drift_detected else "skip_retraining"
    return "retrain_model" if drift_detected else "test_prediction"

# def preprocess_data_2(df, target, reference_features=None):
#     """Preprocess the dataset by removing columns with high NaN or zero ratios."""
#     nan_threshold = 0.5  # Remove columns with more than 50% NaN values
#     zero_threshold = 0.5  # Remove columns with more than 50% zero values

#     # Calculate the NaN and zero ratios for each column
#     nan_ratios = df.isna().mean()
#     zero_ratios = (df == 0).mean()

#     # Filter out columns exceeding the thresholds
#     filtered_df = df.loc[:, (nan_ratios < nan_threshold) & (zero_ratios < zero_threshold)]

#     # Ensure the target column is included
#     if target not in filtered_df.columns:
#         filtered_df[target] = df[target]

#     # Return the feature columns for reference
#     feature_columns = [col for col in filtered_df.columns if col != target]
#     return filtered_df, target, feature_columns

# from evidently.report import Report
# from evidently.metric_preset import DataDriftPreset

# def check_drift():
#     logging.info("Checking for drift...")

#     # Download reference and current data from MinIO
#     reference_file = "/tmp/Financial_with_Sentiment.csv"
#     current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", reference_file)
#     download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

#     # Load datasets
#     reference_df = pd.read_csv(reference_file)
#     current_df = pd.read_csv(current_file)

#     # Preprocess reference and current data
#     target = "ValueTrading"
#     reference_df, _, feature_columns = preprocess_data_2(reference_df, target)
#     current_df, _, _ = preprocess_data_2(current_df, target, reference_features=feature_columns)

#     # Create Evidently drift report
#     column_mapping = {
#         "target": target,
#         "numerical_features": feature_columns,
#         "categorical_features": []
#     }

#     drift_report = Report(metrics=[DataDriftPreset()])
#     drift_report.run(reference_data=reference_df, current_data=current_df, column_mapping=column_mapping)

#     # Check for drift
#     drift_detected = drift_report.as_dict()["metrics"][0]["result"]["dataset_drift"]
#     logging.info(f"Drift detected: {drift_detected}")

#     # Return action based on drift detection
#     return "retrain_model" if drift_detected else "test_prediction"


def retrain_model():
    logging.info("Starting retraining process...")

    # reference_file = "/tmp/Financial_with_Sentiment.csv"
    # download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment.csv", reference_file)
    current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"
    download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

    reference_df = pd.read_csv(current_file)
    target = "ValueTrading"
    X_scaled, y, _ = preprocess_data(reference_df, target)

    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
    xgb_model.fit(X_train, y_train)

    # model_path = "/tmp/retrained_model.pkl"
    model_path = "/tmp/retrained_model_test.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(xgb_model, f)
    # upload_to_minio(model_path, MINIO_BUCKET, "data/Ingestion/retrained_model.pkl")
    upload_to_minio(model_path, MINIO_BUCKET, "data/Ingestion/retrained_model_test.pkl")
    logging.info("Retrained model saved and uploaded.")

    y_pred = xgb_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    # logging.info(f"Retrained Model Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
    # print(f"Retrained Model Metrics:\nMSE: {mse:.2f}\nR2: {r2:.2f}")

def test_prediction_after_retrain():
    logging.info("Testing prediction on current data...")

    current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"
    download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

    # model_path = "/tmp/retrained_model.pkl"
    # download_from_minio(MINIO_BUCKET, "data/Ingestion/retrained_model.pkl", model_path)
    model_path = "/tmp/retrained_model_test.pkl"
    download_from_minio(MINIO_BUCKET, "data/Ingestion/retrained_model_test.pkl", model_path)
    with open(model_path, "rb") as f:
        xgb_model = pickle.load(f)

    current_df = pd.read_csv(current_file)
    target = "ValueTrading"

    _, _, feature_columns = preprocess_data(current_df, target)
    current_X_scaled, current_y, _ = preprocess_data(current_df, target, reference_features=feature_columns)

    y_pred = xgb_model.predict(current_X_scaled)

    mse = mean_squared_error(current_y, y_pred)
    r2 = r2_score(current_y, y_pred)
    # logging.info(f"Test Prediction Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
    # print(f"Test Prediction Metrics:\nMSE: {mse:.2f}\nR2: {r2:.2f}")

def test_prediction_without_retrain():
    logging.info("Testing prediction on current data...")

    current_file = "/tmp/Financial_with_Sentiment_pseudo.csv"
    download_from_minio(MINIO_BUCKET, "data/Ingestion/Financial_with_Sentiment_pseudo.csv", current_file)

    # model_path = "/tmp/retrained_model.pkl"
    # download_from_minio(MINIO_BUCKET, "data/Ingestion/retrained_model.pkl", model_path)
    model_path = "/tmp/retrained_model_test.pkl"
    download_from_minio(MINIO_BUCKET, "data/Ingestion/retrained_model_test.pkl", model_path)
    with open(model_path, "rb") as f:
        xgb_model = pickle.load(f)

    current_df = pd.read_csv(current_file)
    target = "ValueTrading"

    _, _, feature_columns = preprocess_data(current_df, target)
    current_X_scaled, current_y, _ = preprocess_data(current_df, target, reference_features=feature_columns)

    y_pred = xgb_model.predict(current_X_scaled)

    mse = mean_squared_error(current_y, y_pred)
    r2 = r2_score(current_y, y_pred)
    # logging.info(f"Test Prediction Evaluation - MSE: {mse:.2f}, R2: {r2:.2f}")
    # print(f"Test Prediction Metrics:\nMSE: {mse:.2f}\nR2: {r2:.2f}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

current_date = datetime.now()

dag = DAG(
    dag_id="xgboost_drift_pipeline",
    default_args=default_args,
    description="XGBoost pipeline with drift detection, retraining, and testing",
    schedule="@hourly",
    start_date=current_date,
    catchup=False,
)

# check_drift_task = BranchPythonOperator(
#     task_id="check_drift",
#     python_callable=check_drift,
#     dag=dag,
# )

# retrain_task = PythonOperator(
#     task_id="retrain_model",
#     python_callable=retrain_model,
#     dag=dag,
# )

# skip_retrain_task = DummyOperator(
#     task_id="skip_retraining",
#     dag=dag,
# )

# test_prediction_task = PythonOperator(
#     task_id="test_prediction",
#     python_callable=test_prediction,
#     dag=dag,
# )

# merge_task = DummyOperator(
#     task_id="merge_task",
#     dag=dag,
#     trigger_rule="none_failed_min_one_success",  # Ensure it runs if at least one upstream task succeeds
# )

# check_drift_task >> [retrain_task, skip_retrain_task]
# retrain_task >> merge_task
# skip_retrain_task >> merge_task
# merge_task >> test_prediction_task

# Tasks
check_drift_task = BranchPythonOperator(
    task_id="check_drift",
    python_callable=check_drift,
    dag=dag,
)

retrain_task = PythonOperator(
    task_id="retrain_model",
    python_callable=retrain_model,
    dag=dag,
)

test_prediction_after_retrain_task = PythonOperator(
    task_id="test_prediction_after_retrain",
    python_callable=test_prediction_after_retrain,
    dag=dag,
)

test_prediction_without_retrain_task = PythonOperator(
    task_id="test_prediction_without_retrain",
    python_callable=test_prediction_without_retrain,
    dag=dag,
)

# Dependencies
check_drift_task >> [retrain_task, test_prediction_without_retrain_task]
retrain_task >> test_prediction_after_retrain_task