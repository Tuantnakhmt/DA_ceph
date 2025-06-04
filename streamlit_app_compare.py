import streamlit as st
import pandas as pd
import boto3
import os

# Cấu hình MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")
FINANCIAL_FILE_PATH = os.getenv("FINANCIAL_FILE_PATH", "data/Ingestion/Compare/Financial_Year.xlsx")

# Khởi tạo client MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)
@st.cache_data
# Hàm tải tệp từ MinIO
def download_from_minio(bucket, key, local_path):
    with open(local_path, 'wb') as f:
        s3_client.download_fileobj(bucket, key, f)
    return local_path

# Giao diện Streamlit
st.title("Trực quan hóa dữ liệu tài chính")
st.markdown("### Hiển thị dữ liệu tài chính từ MinIO")

# Tải tệp dữ liệu tài chính từ MinIO
local_file_path = "/tmp/Financial_Year.xlsx"
try:
    st.info("Đang tải dữ liệu tài chính từ MinIO...")
    download_from_minio(MINIO_BUCKET, FINANCIAL_FILE_PATH, local_file_path)
    st.success(f"Tệp đã được tải thành công: {local_file_path}")
except Exception as e:
    st.error(f"Lỗi khi tải tệp: {e}")
    st.stop()

# Đọc tệp Excel
st.info("Đang tải dữ liệu tài chính...")
try:
    df = pd.read_excel(local_file_path)
    st.markdown("### Dữ liệu tài chính (5 dòng đầu tiên):")
    st.dataframe(df.head())  # Hiển thị 5 dòng đầu tiên của dữ liệu

    # Thống kê tổng quát
    st.markdown("### Thống kê tổng quát:")
    st.write(df.describe())

    # Phân bố giá trị cột 'Compare'
    st.markdown("### Phân bố giá trị trong cột 'Compare':")
    if 'Compare' in df.columns:
        compare_counts = df['Compare'].value_counts()
        st.bar_chart(compare_counts)
        st.write("**Số lượng các giá trị trong cột 'Compare':**")
        st.write(compare_counts)

    # # Dữ liệu bị thiếu
    # st.markdown("### Dữ liệu bị thiếu:")
    missing_data = df.isnull().sum()
    st.write("**Số lượng giá trị bị thiếu theo từng cột:**")
    st.write(missing_data)
except Exception as e:
    st.error(f"Lỗi khi xử lý tệp: {e}")
