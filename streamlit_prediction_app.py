import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime

import boto3
import os
from io import StringIO

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

@st.cache_data
def load_data_from_minio(file_key):
    """Load data from MinIO."""
    response = s3.get_object(Bucket=MINIO_BUCKET, Key=file_key)
    data = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    return data

st.title("Dashboard Phân Tích Dự Đoán")
st.write("Bảng điều khiển tự động cập nhật kết quả dự đoán, phân tích phần dư và độ quan trọng đặc trưng.")

# Load Data from MinIO
st.info("Đang tải dữ liệu từ MinIO...")
try:
    # data = load_data_from_minio("data/Ingestion/Financial_with_Sentiment.csv")
    data = load_data_from_minio("data/Ingestion/Financial_with_Sentiment.csv")
    data['Time_Close'] = pd.to_datetime(data['Time_Close'], errors='coerce')
    data['ValueTrading'] = pd.to_numeric(data['ValueTrading'], errors='coerce')
    # data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'].str.replace(',', ''), errors='coerce')
    # Ensure VolumeTrading can handle non-string values
    if data['VolumeTrading'].dtype == 'object':
        data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'].str.replace(',', ''), errors='coerce')
    else:
        data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'], errors='coerce')

    st.success("Dữ liệu đã được tải thành công!")
except Exception as e:
    st.error(f"Lỗi khi tải dữ liệu từ MinIO: {e}")
    st.stop()

# Sidebar Filters
st.sidebar.header("Bộ lọc dữ liệu")
years = st.sidebar.multiselect("Chọn năm", options=data['Year'].dropna().unique(), default=data['Year'].dropna().unique())
date_range = st.sidebar.date_input(
    "Chọn khoảng thời gian",
    value=(data['Time_Close'].min(), data['Time_Close'].max()),
    min_value=data['Time_Close'].min(),
    max_value=data['Time_Close'].max(),
)
start_date = datetime.datetime.combine(date_range[0], datetime.time.min)
end_date = datetime.datetime.combine(date_range[1], datetime.time.max)
filtered_data = data[(data['Year'].isin(years)) & (data['Time_Close'].between(start_date, end_date))]

@st.cache_data
def load_model_outputs():
    # predictions = load_data_from_minio("data/Ingestion/predictions.csv")
    # feature_importance = load_data_from_minio("data/Ingestion/feature_importance.csv")
    # response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/metrics.txt")
    predictions = load_data_from_minio("data/Ingestion/predictions_1.csv")
    feature_importance = load_data_from_minio("data/Ingestion/feature_importance_1.csv")
    response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/metrics_1.txt")
    metrics = response['Body'].read().decode('utf-8').splitlines()
    mse = float(metrics[0].split(": ")[1])
    r2 = float(metrics[1].split(": ")[1])
    return predictions, feature_importance, mse, r2

refresh_interval = 10  # seconds
placeholder = st.empty()

while True:
    with placeholder.container():
        predictions, feature_importance, mse, r2 = load_model_outputs()

        key_suffix = str(int(time.time()))

        st.header("Biểu diễn sự phân tán sai lệch dự đoán")
        fig_actual_vs_pred = plt.figure(figsize=(8, 6))
        plt.scatter(predictions["True Values"], predictions["Predicted Values"], alpha=0.6, label="Điểm biểu diễn")
        plt.plot([predictions["True Values"].min(), predictions["True Values"].max()],
                 [predictions["True Values"].min(), predictions["True Values"].max()],
                 color="red", label="Đường lý tưởng")
        plt.title("Biểu diễn sự phân tán sai lệch dự đoán")
        plt.xlabel("Giá trị thực")
        plt.ylabel("Giá trị dự đoán")
        plt.legend()
        st.pyplot(fig_actual_vs_pred)

        st.header("Phân Phối Phần Dư")
        fig_residuals = plt.figure(figsize=(8, 6))
        plt.hist(predictions["Residuals"], bins=30, alpha=0.7, color="blue")
        plt.title("Phân Phối Phần Dư")
        plt.xlabel("Residuals")
        plt.ylabel("Frequency")
        st.pyplot(fig_residuals)

        st.header("Đánh giá Mô Hình")
        col1, col2 = st.columns(2)
        col1.metric("Lỗi Trung Bình Bình Phương (MSE)", f"{mse:.2f}")
        col2.metric("Hệ Số Xác Định (R²)", f"{r2:.2f}")


        st.header("Tính Quan Trọng Của Đặc Trưng")

        top_features = feature_importance.sort_values(by="Importance", ascending=False).head(20)

        fig_feature_importance = plt.figure(figsize=(8, 6))
        plt.barh(top_features["Feature"], top_features["Importance"], color="green", alpha=0.7)
        plt.title("Top 10 Đặc Trưng Quan Trọng")
        plt.xlabel("Độ Quan Trọng")
        plt.ylabel("Đặc Trưng")
        plt.gca().invert_yaxis() 
        st.pyplot(fig_feature_importance)


        st.write(f"🔄 Dashboard cập nhật lúc: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    time.sleep(refresh_interval)
