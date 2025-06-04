import os
import boto3
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Cấu hình MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")
SUMMARY_LOG_PATH = "data/Ingestion/Close/CafeF/summary_log.csv"

# Khởi tạo MinIO Client
def init_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

# Tải tệp summary_log từ MinIO
@st.cache_data
def download_summary_log():
    client = init_minio_client()
    try:
        obj = client.get_object(Bucket=MINIO_BUCKET, Key=SUMMARY_LOG_PATH)
        return pd.read_csv(obj["Body"])
    except Exception as e:
        st.error(f"Không thể tải tệp summary_log từ MinIO: {str(e)}")
        return pd.DataFrame()

# Ứng dụng Streamlit
def main():
    st.title("Tóm tắt quá trình thu thập dữ liệu")
    st.markdown("Bảng điều khiển cung cấp thông tin chi tiết về quá trình thu thập dữ liệu.")

    # Tải tệp summary_log
    summary_log = download_summary_log()

    if summary_log.empty:
        st.warning("Không có dữ liệu trong tệp summary_log.")
        return

    # Hiển thị dữ liệu thô
    st.subheader("Dữ liệu tóm tắt")
    st.dataframe(summary_log)

    # Thống kê
    st.subheader("Thống kê chính")
    total_symbols = len(summary_log)
    successful_crawls = len(summary_log[summary_log["crawl_status"] == "Success"])
    failed_crawls = len(summary_log[summary_log["crawl_status"] == "Failed"])
    no_data_crawls = len(summary_log[summary_log["crawl_status"] == "No Data"])
    important_missing = len(summary_log[summary_log["is_important_column_missing"] == True])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tổng số mã cổ phiếu", total_symbols)
    col2.metric("Thu thập thành công", successful_crawls)
    col3.metric("Thu thập thất bại", failed_crawls)
    col4.metric("Thiếu dữ liệu quan trọng", important_missing)

    # Lọc dữ liệu
    st.subheader("Tùy chọn lọc")
    status_filter = st.multiselect(
        "Lọc theo trạng thái thu thập",
        options=summary_log["crawl_status"].unique(),
        default=summary_log["crawl_status"].unique()
    )

    missing_filter = st.selectbox(
        "Lọc theo cột quan trọng bị thiếu",
        options=["Tất cả", "Có", "Không"],
        index=0
    )

    filtered_log = summary_log[
        summary_log["crawl_status"].isin(status_filter)
    ]

    if missing_filter != "Tất cả":
        filtered_log = filtered_log[
            filtered_log["is_important_column_missing"] == (missing_filter == "Có")
        ]

    st.subheader("Dữ liệu đã lọc")
    st.dataframe(filtered_log)

    # Biểu đồ
    st.subheader("Biểu đồ")
    chart_type = st.selectbox("Chọn loại biểu đồ", ["Biểu đồ cột", "Biểu đồ tròn"])

    if chart_type == "Biểu đồ cột":
        st.bar_chart(filtered_log["crawl_status"].value_counts())
    elif chart_type == "Biểu đồ tròn":
        fig, ax = plt.subplots()
        filtered_log["crawl_status"].value_counts().plot.pie(
            autopct='%1.1f%%', ax=ax, startangle=90
        )
        ax.set_ylabel("")
        st.pyplot(fig)

if __name__ == "__main__":
    main()
