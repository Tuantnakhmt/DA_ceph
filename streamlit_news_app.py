# import streamlit as st
# import pandas as pd
# from io import StringIO
# import boto3
# import os

# st.set_page_config(page_title="Trực quan hóa quá trình thu thập tin tức từ CafeF", layout="wide")
# st.title("Trực quan hóa quá trình thu thập tin tức từ CafeF")
# st.write("Hiển thị thông tin crawl tin tức từ MinIO")

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

# st.info("Đang tải dữ liệu log từ MinIO...")
# try:
#     response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/crawl_logs.csv")
#     crawl_logs = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
#     st.success("Dữ liệu log đã được tải thành công!")
# except Exception as e:
#     st.error(f"Lỗi khi tải dữ liệu log từ MinIO: {e}")
#     st.stop()

# st.header("Tổng quan dữ liệu")
# st.dataframe(crawl_logs)

# st.sidebar.header("Lọc dữ liệu")
# selected_status = st.sidebar.selectbox("Trạng thái", options=["Tất cả", "Success", "No News Found", "Failed"])
# if selected_status != "Tất cả":
#     crawl_logs = crawl_logs[crawl_logs["Status"] == selected_status]

# st.subheader("Thống kê trạng thái crawl")
# status_count = crawl_logs["Status"].value_counts().reset_index()
# status_count.columns = ["Trạng thái", "Số lượng"]
# st.bar_chart(status_count.set_index("Trạng thái"))

# st.subheader("Thông tin chi tiết về các mã đã crawl thành công")
# success_logs = crawl_logs[crawl_logs["Status"] == "Success"]
# st.write(f"Tổng số mã thành công: {len(success_logs)}")
# st.dataframe(success_logs)

# if st.sidebar.button("Lưu dữ liệu đã lọc"):
#     filtered_csv = crawl_logs.to_csv(index=False).encode('utf-8')
#     st.sidebar.download_button(
#         label="Tải xuống log đã lọc",
#         data=filtered_csv,
#         file_name="filtered_crawl_logs.csv",
#         mime="text/csv",
#     )

# st.success("Ứng dụng đã chạy thành công!")
# import streamlit as st
# import pandas as pd
# from io import StringIO
# import boto3
# import os

# st.set_page_config(page_title="Tin tức CafeF", layout="wide")
# st.title("Trực quan hóa thông tin thu thập tin tức từ CafeF")

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
# def load_crawl_logs():
#     try:
#         response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/crawl_logs.csv")
#         logs_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
#         return logs_df
#     except Exception as e:
#         st.error(f"Lỗi khi tải dữ liệu log từ MinIO: {e}")
#         return pd.DataFrame()

# crawl_logs = load_crawl_logs()

# if crawl_logs.empty:
#     st.error("Không có dữ liệu log để hiển thị.")
#     st.stop()

# st.metric("Tổng số tin tức đã thu thập", int(crawl_logs["News Count"].sum()))

# with st.sidebar:
#     st.header("Bộ lọc")
#     selected_status = st.multiselect(
#         "Trạng thái", options=crawl_logs["Status"].unique(), default=crawl_logs["Status"].unique()
#     )
#     filter_news_count = st.slider(
#         "Số lượng tin tức", min_value=int(crawl_logs["News Count"].min()),
#         max_value=int(crawl_logs["News Count"].max()),
#         value=(int(crawl_logs["News Count"].min()), int(crawl_logs["News Count"].max()))
#     )

# filtered_logs = crawl_logs[
#     (crawl_logs["Status"].isin(selected_status)) &
#     (crawl_logs["News Count"].between(filter_news_count[0], filter_news_count[1]))
# ]

# st.subheader("Dữ liệu log đã thu thập")
# st.write("**Dữ liệu đã được lọc dựa trên các tùy chọn:**")
# st.dataframe(filtered_logs, use_container_width=True)

# filtered_csv = filtered_logs.to_csv(index=False).encode('utf-8')
# st.download_button(
#     label="Tải xuống log đã lọc",
#     data=filtered_csv,
#     file_name="filtered_crawl_logs.csv",
#     mime="text/csv"
# )

# st.subheader("Thống kê trạng thái thu thập")
# status_summary = (
#     filtered_logs.groupby("Status")["Symbol"]
#     .count()
#     .reset_index()
#     .rename(columns={"Symbol": "Số lượng công ty"})
# )
# st.bar_chart(status_summary.set_index("Status"))

import streamlit as st
import pandas as pd
from io import StringIO
import boto3
import os

st.set_page_config(page_title="Trực quan hóa quá trình thu thập tin tức từ CafeF", layout="wide")
st.title("Trực quan hóa quá trình thu thập tin tức từ CafeF")
st.write("Hiển thị thông tin crawl tin tức từ MinIO")

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

st.info("Đang tải dữ liệu log từ MinIO...")
try:
    response = s3.get_object(Bucket=MINIO_BUCKET, Key="data/Ingestion/crawl_logs.csv")
    crawl_logs = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    st.success("Dữ liệu log đã được tải thành công!")
except Exception as e:
    st.error(f"Lỗi khi tải dữ liệu log từ MinIO: {e}")
    st.stop()

st.header("Tổng quan dữ liệu")
st.dataframe(crawl_logs)
st.metric("Tổng số tin tức đã thu thập", int(crawl_logs["News Count"].sum())+1)
# Filters in the main content area
st.subheader("Tùy chọn lọc")

# Filter by Status
selected_status = st.selectbox(
    "Lọc theo trạng thái", options=["Tất cả"] + crawl_logs["Status"].unique().tolist()
)
if selected_status != "Tất cả":
    crawl_logs = crawl_logs[crawl_logs["Status"] == selected_status]

# Display Filtered Data
st.subheader("Dữ liệu đã lọc")
st.dataframe(crawl_logs)

# Statistics Section
st.subheader("Thống kê trạng thái crawl")
status_count = crawl_logs["Status"].value_counts().reset_index()
status_count.columns = ["Trạng thái", "Số lượng"]
st.bar_chart(status_count.set_index("Trạng thái"))

# Save filtered data
if st.button("Lưu dữ liệu đã lọc"):
    filtered_csv = crawl_logs.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Tải xuống log đã lọc",
        data=filtered_csv,
        file_name="filtered_crawl_logs.csv",
        mime="text/csv",
    )

st.success("Ứng dụng đã chạy thành công!")
