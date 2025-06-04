# import streamlit as st
# import pandas as pd
# import matplotlib.pyplot as plt
# import time
# import datetime

# import boto3
# import os
# from io import StringIO

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# s3 = boto3.client(
#     's3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# @st.cache_data
# def load_data_from_minio(file_key):
#     """Load data from MinIO."""
#     response = s3.get_object(Bucket=MINIO_BUCKET, Key=file_key)
#     data = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
#     return data

# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# # VADER Sentiment Analyzer
# analyzer = SentimentIntensityAnalyzer()

# st.title("Dashboard Phân Tích Dự Đoán")
# st.write("Bảng điều khiển tự động cập nhật kết quả dự đoán, phân tích phần dư và độ quan trọng đặc trưng.")

# # Load Data from MinIO
# st.info("Đang tải dữ liệu từ MinIO...")
# try:
#     data = load_data_from_minio("data/Ingestion/Financial_with_Sentiment.csv")
#     data['Time_Close'] = pd.to_datetime(data['Time_Close'], errors='coerce')
#     data['ValueTrading'] = pd.to_numeric(data['ValueTrading'], errors='coerce')
#     # data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'].str.replace(',', ''), errors='coerce')
#     # Ensure VolumeTrading can handle non-string values
#     if data['VolumeTrading'].dtype == 'object':
#         data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'].str.replace(',', ''), errors='coerce')
#     else:
#         data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'], errors='coerce')

#     st.success("Dữ liệu đã được tải thành công!")
# except Exception as e:
#     st.error(f"Lỗi khi tải dữ liệu từ MinIO: {e}")
#     st.stop()

# # User Input Section for Feedback
# st.header("Cập Nhật Điểm Sentiment")
# symbol = st.text_input("Nhập mã chứng khoán (Symbol):", value="")
# statement = st.text_area("Nhập nhận xét của bạn về mã chứng khoán:")

# # if st.button("Cập Nhật Điểm Sentiment"):
# #     if not symbol or not statement:
# #         st.error("Vui lòng nhập cả mã chứng khoán và nhận xét!")
# #     else:
# #         # Calculate sentiment score
# #         sentiment = analyzer.polarity_scores(statement)
# #         sentiment_score = sentiment["compound"]  # Use compound score as overall sentiment

# #         # Update the dataset
# #         try:
# #             # Locate the row for the given symbol and update sentiment_score
# #             if symbol in data["Symbol"].values:
# #                 data.loc[data["Symbol"] == symbol, "sentiment_score"] = sentiment_score
# #                 st.success(f"Điểm sentiment cho mã {symbol} đã được cập nhật thành công!")
                
# #                 # Save the updated dataset back to MinIO
# #                 csv_buffer = StringIO()
# #                 data.to_csv(csv_buffer, index=False)
# #                 s3.put_object(
# #                     Bucket=MINIO_BUCKET,
# #                     Key="data/Ingestion/Financial_with_Sentiment.csv",
# #                     Body=csv_buffer.getvalue()
# #                 )
# #                 st.info("Dataset đã được lưu lại trên MinIO.")
# #             else:
# #                 st.error(f"Mã chứng khoán {symbol} không tồn tại trong dữ liệu!")
# #         except Exception as e:
# #             st.error(f"Đã xảy ra lỗi khi cập nhật điểm sentiment: {e}")
# if st.button("Cập Nhật Điểm Sentiment"):
#     if not symbol or not statement:
#         st.error("Vui lòng nhập cả mã chứng khoán và nhận xét!")
#     else:
#         # Check if the symbol exists in the dataset
#         if symbol in data["Symbol"].values:
#             # Fetch the current sentiment score for the symbol
#             current_score = data.loc[data["Symbol"] == symbol, "sentiment_score"].values[0]

#             # Calculate the new sentiment score
#             sentiment = analyzer.polarity_scores(statement)
#             new_score = sentiment["compound"]  # Use compound score as overall sentiment

#             # Display before and after scores
#             st.info(f"Điểm sentiment trước khi cập nhật: **{current_score:.4f}**")
#             st.info(f"Điểm sentiment sau khi cập nhật: **{new_score:.4f}**")

#             # Update the sentiment score in the dataset
#             try:
#                 data.loc[data["Symbol"] == symbol, "sentiment_score"] = new_score
#                 st.success(f"Điểm sentiment cho mã {symbol} đã được cập nhật thành công!")

#                 # Save the updated dataset back to MinIO
#                 csv_buffer = StringIO()
#                 data.to_csv(csv_buffer, index=False)
#                 s3.put_object(
#                     Bucket=MINIO_BUCKET,
#                     Key="data/Ingestion/Financial_with_Sentiment.csv",
#                     Body=csv_buffer.getvalue()
#                 )
#                 st.info("Dataset đã được lưu lại trên MinIO.")
#             except Exception as e:
#                 st.error(f"Đã xảy ra lỗi khi cập nhật điểm sentiment: {e}")
#         else:
#             st.error(f"Mã chứng khoán {symbol} không tồn tại trong dữ liệu!")

# # Load Data from MinIO
# st.info("Đang tải dữ liệu từ MinIO...")
# try:
#     data = load_data_from_minio("data/Ingestion/Financial_with_Sentiment.csv")
#     data['Time_Close'] = pd.to_datetime(data['Time_Close'], errors='coerce')
#     data['ValueTrading'] = pd.to_numeric(data['ValueTrading'], errors='coerce')
#     if data['VolumeTrading'].dtype == 'object':
#         data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'].str.replace(',', ''), errors='coerce')
#     else:
#         data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'], errors='coerce')

#     st.success("Dữ liệu đã được tải thành công!")
# except Exception as e:
#     st.error(f"Lỗi khi tải dữ liệu từ MinIO: {e}")
#     st.stop()

# Rest of the dashboard code (visualizations, filters, etc.)


import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime

import boto3
import os
from io import StringIO
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

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

def backup_dataset(dataset, backup_key):
    """Backup the current dataset to MinIO."""
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{backup_key}_{timestamp}.csv"
        
        csv_buffer = StringIO()
        dataset.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=f"backups/{backup_filename}",
            Body=csv_buffer.getvalue()
        )
        st.info(f"Backup created: {backup_filename}")
    except Exception as e:
        st.error(f"Failed to create backup: {e}")

def list_backups():
    """List all available backups in the MinIO backups folder."""
    try:
        response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="backups/")
        if "Contents" in response:
            return [obj["Key"] for obj in response["Contents"]]
        else:
            return []
    except Exception as e:
        st.error(f"Failed to list backups: {e}")
        return []

def load_backup(backup_key):
    """Load a specific backup from MinIO."""
    try:
        response = s3.get_object(Bucket=MINIO_BUCKET, Key=backup_key)
        backup_data = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
        return backup_data
    except Exception as e:
        st.error(f"Failed to load backup: {e}")
        return None

# VADER Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

st.title("Dashboard cập nhật điểm cảm xúc tin tức (Sentiment)")
st.write("Bảng điều cho phép cập nhật trực tiếp điểm cảm xúc đối với tin tức, có thể cập nhật lại dữ liệu cũ nếu cần thiết.")

# Load Data from MinIO
st.info("Đang tải dữ liệu từ MinIO...")
try:
    data = load_data_from_minio("data/Ingestion/Financial_with_Sentiment.csv")
    data['Time_Close'] = pd.to_datetime(data['Time_Close'], errors='coerce')
    data['ValueTrading'] = pd.to_numeric(data['ValueTrading'], errors='coerce')
    if data['VolumeTrading'].dtype == 'object':
        data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'].str.replace(',', ''), errors='coerce')
    else:
        data['VolumeTrading'] = pd.to_numeric(data['VolumeTrading'], errors='coerce')

    st.success("Dữ liệu đã được tải thành công!")
except Exception as e:
    st.error(f"Lỗi khi tải dữ liệu từ MinIO: {e}")
    st.stop()

# User Input Section for Feedback
st.header("Cập Nhật Điểm Sentiment")
symbol = st.text_input("Nhập mã chứng khoán (Symbol):", value="")
statement = st.text_area("Nhập nhận xét của bạn về mã chứng khoán:")

if st.button("Cập Nhật Điểm Sentiment"):
    if not symbol or not statement:
        st.error("Vui lòng nhập cả mã chứng khoán và nhận xét!")
    else:
        # Check if the symbol exists in the dataset
        if symbol in data["Symbol"].values:
            # Fetch the current sentiment score for the symbol
            current_score = data.loc[data["Symbol"] == symbol, "sentiment_score"].values[0]

            # Calculate the new sentiment score
            sentiment = analyzer.polarity_scores(statement)
            new_score = sentiment["compound"]  # Use compound score as overall sentiment

            # Display before and after scores
            st.info(f"Điểm sentiment trước khi cập nhật: **{current_score:.4f}**")
            st.info(f"Điểm sentiment sau khi cập nhật: **{new_score:.4f}**")

            # Backup current dataset
            backup_dataset(data, "Financial_with_Sentiment")

            # Update the sentiment score in the dataset
            try:
                data.loc[data["Symbol"] == symbol, "sentiment_score"] = new_score
                st.success(f"Điểm sentiment cho mã {symbol} đã được cập nhật thành công!")

                # Save the updated dataset back to MinIO
                csv_buffer = StringIO()
                data.to_csv(csv_buffer, index=False)
                s3.put_object(
                    Bucket=MINIO_BUCKET,
                    Key="data/Ingestion/Financial_with_Sentiment.csv",
                    Body=csv_buffer.getvalue()
                )
                st.info("Dataset đã được lưu lại trên MinIO.")
            except Exception as e:
                st.error(f"Đã xảy ra lỗi khi cập nhật điểm sentiment: {e}")
        else:
            st.error(f"Mã chứng khoán {symbol} không tồn tại trong dữ liệu!")

# Recovery UI
st.sidebar.header("Khôi phục dữ liệu")
backups = list_backups()
if backups:
    selected_backup = st.sidebar.selectbox("Chọn backup để khôi phục:", backups)
    if st.sidebar.button("Khôi phục backup"):
        backup_data = load_backup(selected_backup)
        if backup_data is not None:
            # Replace current dataset with the backup
            data = backup_data
            
            # Save the restored dataset back to MinIO
            try:
                csv_buffer = StringIO()
                data.to_csv(csv_buffer, index=False)
                s3.put_object(
                    Bucket=MINIO_BUCKET,
                    Key="data/Ingestion/Financial_with_Sentiment.csv",
                    Body=csv_buffer.getvalue()
                )
                st.success(f"Dữ liệu đã được khôi phục từ backup và lưu lại thành công: {selected_backup}")
            except Exception as e:
                st.error(f"Đã xảy ra lỗi khi lưu dataset được khôi phục: {e}")
else:
    st.sidebar.info("Không có bản backup nào.")

