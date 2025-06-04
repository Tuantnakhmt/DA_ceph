# # import os
# # import boto3
# # import logging

# # # Set up logging
# # # logging.basicConfig(level=logging.INFO)
# # # logger = logging.getLogger(__name__)

# # # MinIO Configuration
# # MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# # MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# # MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# # MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")
# # BASE_LOCAL_PATH = os.getenv("BASE_LOCAL_PATH", "/app/data/Ingestion")  # Change to match your environment
# # TARGET_MINIO_FOLDER = "data/Ingestion"  # Target folder in MinIO

# # # Initialize MinIO Client
# # minio_client = boto3.client(
# #     's3',
# #     endpoint_url=MINIO_ENDPOINT,
# #     aws_access_key_id=MINIO_ACCESS_KEY,
# #     aws_secret_access_key=MINIO_SECRET_KEY
# # )

# # # Ensure MinIO Bucket Exists
# # def ensure_bucket(bucket_name):
# #     try:
# #         minio_client.head_bucket(Bucket=bucket_name)
# #         print(f"Bucket '{bucket_name}' exists.")
# #     except Exception:
# #         print(f"Creating bucket '{bucket_name}'.")
# #         minio_client.create_bucket(Bucket=bucket_name)

# # # Upload File to MinIO
# # def upload_file_to_minio(local_file_path, target_folder, bucket_name):
# #     try:
# #         file_name = os.path.basename(local_file_path)
# #         key = f"{target_folder}/{file_name}"

# #         with open(local_file_path, "rb") as file_data:
# #             minio_client.put_object(
# #                 Bucket=bucket_name,
# #                 Key=key,
# #                 Body=file_data
# #             )
# #         print(f"Uploaded {key} to MinIO.")
# #     except Exception as e:
# #         print(f"Failed to upload {local_file_path} to MinIO: {str(e)}")

# # # Traverse and Upload Folder
# # def upload_folder_to_minio(local_folder_path, target_folder, bucket_name):
# #     for root, dirs, files in os.walk(local_folder_path):
# #         for file in files:
# #             local_file_path = os.path.join(root, file)
# #             relative_path = os.path.relpath(local_file_path, local_folder_path)
# #             minio_target_path = os.path.join(target_folder, relative_path).replace("\\", "/")
# #             upload_file_to_minio(local_file_path, minio_target_path, bucket_name)

# # # Main Function
# # if __name__ == "__main__":
# #     ensure_bucket(MINIO_BUCKET)

# #     folders_to_upload = ["Financial", "Compare"]
# #     for folder in folders_to_upload:
# #         local_folder_path = os.path.join(BASE_LOCAL_PATH, folder)
# #         minio_target_folder = os.path.join(TARGET_MINIO_FOLDER, folder).replace("\\", "/")
# #         print(f"Uploading folder '{folder}' to MinIO...")
# #         upload_folder_to_minio(local_folder_path, minio_target_folder, MINIO_BUCKET)

# #     print("Upload process completed.")

# import os
# import boto3
# import logging

# # Set up logging
# # logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger(__name__)

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")
# BASE_LOCAL_PATH = os.getenv("BASE_LOCAL_PATH", "/app/data/Ingestion")

# # Initialize MinIO Client
# # logger.info("Initializing MinIO client")
# minio_client = boto3.client(
#     's3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY
# )

# # Ensure MinIO Bucket Exists
# def ensure_bucket(bucket_name):
#     try:
#         minio_client.head_bucket(Bucket=bucket_name)
#         # logger.info(f"Bucket '{bucket_name}' exists.")
#     except Exception:
#         # logger.info(f"Creating bucket '{bucket_name}'.")
#         minio_client.create_bucket(Bucket=bucket_name)

# ensure_bucket(MINIO_BUCKET)

# # Upload File to MinIO
# def upload_file_to_minio(local_file_path, remote_file_key):
#     try:
#         with open(local_file_path, "rb") as file_data:
#             minio_client.put_object(
#                 Bucket=MINIO_BUCKET,
#                 Key=remote_file_key,
#                 Body=file_data
#             )
#         print(f"Uploaded {remote_file_key} to MinIO.")
#     except Exception as e:
#         print(f"Failed to upload {local_file_path} to MinIO: {e}")

# # Upload Directory to MinIO
# def upload_directory_to_minio(local_dir_path, remote_dir_key):
#     for root, dirs, files in os.walk(local_dir_path):
#         for file in files:
#             local_file_path = os.path.join(root, file)
#             relative_path = os.path.relpath(local_file_path, local_dir_path)
#             remote_file_key = os.path.join(remote_dir_key, relative_path).replace("\\", "/")
#             upload_file_to_minio(local_file_path, remote_file_key)

# # Main Upload Process
# def main():
#     print("Starting upload process")
#     for folder_name in os.listdir(BASE_LOCAL_PATH):
#         folder_path = os.path.join(BASE_LOCAL_PATH, folder_name)
#         if os.path.isdir(folder_path):
#             print(f"Uploading folder: {folder_name}")
#             upload_directory_to_minio(folder_path, f"data/Ingestion/{folder_name}")
#         else:
#             print(f"Skipping non-folder item: {folder_name}")
#     print("Upload process completed")

# if __name__ == "__main__":
#     main()
