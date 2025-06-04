# import os
# import boto3
# import pandas as pd
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# import time
# import logging

# # Set up logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# # Initialize MinIO Client
# logger.info("Initializing MinIO client")
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
#         logger.info(f"Bucket '{bucket_name}' exists.")
#     except Exception:
#         logger.info(f"Creating bucket '{bucket_name}'.")
#         minio_client.create_bucket(Bucket=bucket_name)

# ensure_bucket(MINIO_BUCKET)

# # Upload DataFrame to MinIO
# def upload_to_minio(dataframe, filename):
#     logger.info(f"Uploading {filename} to MinIO.")
#     csv_data = dataframe.to_csv(index=False).encode('utf-8')
#     minio_client.put_object(
#         Bucket=MINIO_BUCKET,
#         Key=filename,
#         Body=csv_data
#     )
#     logger.info(f"Uploaded {filename} to MinIO.")

# # Selenium-based Crawler
# class StockPriceCrawler:
#     def __init__(self):
#         self.driver = None

#     def reset_driver(self, path=None, source="CF", remote_url="http://selenium:4444/wd/hub"):
#         """
#         Initialize Selenium WebDriver with custom options
#         """
#         logger.info("Initializing Selenium WebDriver")
#         chrome_options = webdriver.ChromeOptions()
#         chrome_options.add_argument('--no-sandbox')
#         chrome_options.add_argument('--disable-dev-shm-usage')
#         chrome_options.add_argument('--disable-gpu')
#         chrome_options.add_argument('--disable-software-rasterizer')
#         chrome_options.add_argument('--headless')
#         chrome_options.add_argument('--disable-extensions')
#         chrome_options.add_argument("--disable-background-networking")
#         chrome_options.add_argument("--disable-vulkan")
#         chrome_options.add_argument('--disable-browser-side-navigation')
#         chrome_options.add_argument('--ignore-certificate-errors')
#         chrome_options.add_argument('--allow-insecure-localhost')
#         chrome_options.add_argument('--ignore-ssl-errors')

#         if source == "CF":
#             chrome_options.add_argument('--start-maximized')
#             chrome_options.add_argument('enable-automation')

#         self.driver = webdriver.Remote(
#             command_executor=remote_url,
#             options=chrome_options
#         )

#         # Additional setup
#         self.driver.set_window_size(1920, 1080)
#         self.driver.implicitly_wait(10)
#         self.driver.set_page_load_timeout(120)
#         logger.info("Selenium WebDriver initialized successfully")

#     def scrape_stock_prices(self, url):
#         """
#         Scrape the final row of stock price data from a multi-page CafeF table
#         """
#         logger.info(f"Scraping data from {url}")
#         if self.driver is None:
#             raise Exception("Driver not initialized. Call reset_driver() first.")

#         self.driver.get(url)
#         time.sleep(5)  # Wait for the table to load

#         final_row_data = None

#         # Navigate through all pages to find the last row
#         while True:
#             rows = self.driver.find_elements(By.XPATH, '//tbody[@id="render-table-owner"]/tr')
#             if rows:
#                 last_row = rows[-1]
#                 cols = last_row.find_elements(By.TAG_NAME, 'td')
#                 final_row_data = [col.text.strip() for col in cols]

#             next_button = self.driver.find_elements(By.XPATH, '//a[contains(@class, "next")]')
#             if next_button and "disabled" not in next_button[0].get_attribute("class"):
#                 next_button[0].click()  # Click "Next" to go to the next page
#                 time.sleep(3)
#             else:
#                 break

#         columns = [
#             "Ngày", "Giá Đóng cửa", "Điều chỉnh", "Thay đổi", 
#             "GD Khối lượng", "GD Giá trị (tỷ VNĐ)", 
#             "GD thỏa thuận Khối lượng", "GD thỏa thuận Giá trị (tỷ VNĐ)", 
#             "Giá Mở cửa", "Giá Cao nhất", "Giá Thấp nhất"
#         ]

#         if final_row_data and len(final_row_data) == len(columns):
#             logger.info(f"Successfully scraped data for {url}")
#             return pd.DataFrame([final_row_data], columns=columns)
#         else:
#             logger.warning(f"No valid data found for URL {url}")
#             return None

#     def close_driver(self):
#         """
#         Close the Selenium WebDriver.
#         """
#         if self.driver:
#             logger.info("Closing Selenium WebDriver")
#             self.driver.quit()

# # Crawl and Upload to MinIO
# def crawl_and_upload(symbol, crawler):
#     """
#     Crawl stock data for a given symbol and upload it to MinIO.
#     """
#     url = f"https://s.cafef.vn/Lich-su-giao-dich-{symbol}-1.chn"
#     df = crawler.scrape_stock_prices(url)

#     if df is not None and not df.empty:
#         filename = f"{symbol}.csv"
#         upload_to_minio(df, filename)
#     else:
#         logger.info(f"No data found for {symbol}.")

# # Main Entry
# if __name__ == "__main__":
#     logger.info("Starting stock price crawl process")

#     # List of stock symbols to crawl
#     symbols = ["AAA"]  # Replace with your symbols

#     crawler = StockPriceCrawler()
#     crawler.reset_driver()

#     try:
#         for symbol in symbols:
#             logger.info(f"Processing {symbol}...")
#             crawl_and_upload(symbol, crawler)
#     finally:
#         crawler.close_driver()

#     logger.info("Crawl process completed.")

# import os
# import boto3
# import pandas as pd
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# import time
# import logging

# # Set up logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# # Initialize MinIO Client
# logger.info("Initializing MinIO client")
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
#         logger.info(f"Bucket '{bucket_name}' exists.")
#     except Exception:
#         logger.info(f"Creating bucket '{bucket_name}'.")
#         minio_client.create_bucket(Bucket=bucket_name)

# ensure_bucket(MINIO_BUCKET)

# # Upload File to MinIO
# def upload_to_minio(bucket, folder_path, filename, data, is_csv=False):
#     key = f"{folder_path}/{filename}"
#     if is_csv:
#         data = data.to_csv(index=False).encode('utf-8')
#     else:
#         data = data.encode('utf-8')
#     minio_client.put_object(
#         Bucket=bucket,
#         Key=key,
#         Body=data
#     )
#     logger.info(f"Uploaded {key} to MinIO.")

# # Upload Local File to MinIO
# def upload_local_file_to_minio(bucket, folder_path, local_file_path):
#     file_name = os.path.basename(local_file_path)
#     key = f"{folder_path}/{file_name}"
#     with open(local_file_path, "rb") as file_data:
#         minio_client.put_object(
#             Bucket=bucket,
#             Key=key,
#             Body=file_data,
#             ContentType="text/csv"
#         )
#     logger.info(f"Uploaded {file_name} to MinIO at {folder_path}/{file_name}")

# # Download File from MinIO
# def download_file_from_minio(bucket, file_key, local_file_path):
#     minio_client.download_file(Bucket=bucket, Key=file_key, Filename=local_file_path)
#     logger.info(f"Downloaded {file_key} from MinIO to {local_file_path}")

# # Selenium-based Crawler
# class StockPriceCrawler:
#     def __init__(self):
#         self.driver = None

#     def reset_driver(self, remote_url="http://selenium:4444/wd/hub"):
#         logger.info("Initializing Selenium WebDriver")
#         chrome_options = webdriver.ChromeOptions()
#         chrome_options.add_argument('--no-sandbox')
#         chrome_options.add_argument('--disable-dev-shm-usage')
#         chrome_options.add_argument('--disable-gpu')
#         chrome_options.add_argument('--headless')
#         self.driver = webdriver.Remote(
#             command_executor=remote_url,
#             options=chrome_options
#         )
#         self.driver.implicitly_wait(10)
#         logger.info("Selenium WebDriver initialized successfully")

#     def scrape_stock_prices(self, url):
#         logger.info(f"Scraping data from {url}")
#         self.driver.get(url)
#         time.sleep(5)

#         rows = self.driver.find_elements(By.XPATH, '//tbody[@id="render-table-owner"]/tr')
#         data = []
#         for row in rows:
#             cols = row.find_elements(By.TAG_NAME, 'td')
#             data.append([col.text.strip() for col in cols])

#         columns = [
#             "Ngày", "Giá Đóng cửa", "Điều chỉnh", "Thay đổi",
#             "GD Khối lượng", "GD Giá trị (tỷ VNĐ)",
#             "GD thỏa thuận Khối lượng", "GD thỏa thuận Giá trị (tỷ VNĐ)",
#             "Giá Mở cửa", "Giá Cao nhất", "Giá Thấp nhất"
#         ]
#         if data:
#             return pd.DataFrame(data, columns=columns)
#         else:
#             logger.warning(f"No valid data found for URL {url}")
#             return None

#     def close_driver(self):
#         if self.driver:
#             logger.info("Closing Selenium WebDriver")
#             self.driver.quit()

# import json
# # Crawl and Save Data
# # Crawl and Save Data
# def crawl_and_save(company_code, crawler):
#     """
#     Crawl stock data for the given company and upload it to MinIO.
#     """
#     url = f"https://s.cafef.vn/Lich-su-giao-dich-{company_code}-1.chn"
#     df = crawler.scrape_stock_prices(url)

#     # Define folder path in MinIO
#     folder_path = f"data/Ingestion/Close/CafeF"
    
#     if df is not None and not df.empty:
#         # Save stock data as <COMPANY_CODE>.csv
#         csv_filename = f"{company_code}.csv"
#         upload_to_minio(
#             MINIO_BUCKET,
#             folder_path,
#             csv_filename,
#             df,
#             is_csv=True
#         )

#         # Save metadata
#         metadata = {
#             "company_code": company_code,
#             "url": url,
#             "crawl_date": pd.Timestamp.now().isoformat()
#         }
#         upload_to_minio(
#             MINIO_BUCKET,
#             folder_path,
#             f"{company_code}_metadata.json",
#             json.dumps(metadata)
#         )
#         logger.info(f"Data for {company_code} saved successfully.")
#     else:
#         logger.warning(f"No data found for {company_code}.")

# # Main Function
# if __name__ == "__main__":
#     logger.info("Starting stock price crawl process")

#     # Upload List_company.csv to MinIO
#     local_file_path = "List_company.csv"
#     minio_folder_path = "data/Ingestion/Close/CafeF"
#     upload_local_file_to_minio(MINIO_BUCKET, minio_folder_path, local_file_path)

   

#     # Load company codes
#     company_list = pd.read_csv(local_file_path)
#     company_codes = company_list["Mã CK"].tolist()  # Adjust column name

#     crawler = StockPriceCrawler()
#     crawler.reset_driver()

#     try:
#         for company_code in company_codes:
#             logger.info(f"Processing {company_code}...")
#             crawl_and_save(company_code, crawler)
#     finally:
#         crawler.close_driver()

#     logger.info("Crawl process completed.")

import os
import boto3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import logging
import json
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset

# # Set up logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize MinIO Client
#logger.info("Initializing MinIO client")
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure MinIO Bucket Exists
def ensure_bucket(bucket_name):
    try:
        minio_client.head_bucket(Bucket=bucket_name)
        #logger.info(f"Bucket '{bucket_name}' exists.")
        print(f"Bucket '{bucket_name}' exists.")
    except Exception:
        #logger.info(f"Creating bucket '{bucket_name}'.")
        print(f"Creating bucket '{bucket_name}'.")
        minio_client.create_bucket(Bucket=bucket_name)

ensure_bucket(MINIO_BUCKET)

# Upload File to MinIO
def upload_to_minio(bucket, folder_path, filename, data, is_csv=False):
    key = f"{folder_path}/{filename}"
    if is_csv:
        data = data.to_csv(index=False).encode('utf-8')
    else:
        data = data.encode('utf-8')
    minio_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data
    )
    #logger.info(f"Uploaded {key} to MinIO.")
    print(f"Uploaded {key} to MinIO.")
# Upload Local File to MinIO
def upload_local_file_to_minio(bucket, folder_path, local_file_path):
    file_name = os.path.basename(local_file_path)
    key = f"{folder_path}/{file_name}"
    with open(local_file_path, "rb") as file_data:
        minio_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_data,
            ContentType="text/csv"
        )
    #logger.info(f"Uploaded {file_name} to MinIO at {folder_path}/{file_name}")
    print((f"Uploaded {file_name} to MinIO at {folder_path}/{file_name}"))
# Generate Evidently Reports


# def generate_evidently_reports(reference_df, current_df, folder_path, report_type="drift"):
#     """
#     Generate Evidently reports (data drift or data quality) for the given datasets.
#     Save the report as an HTML file and return the local path and file name.
#     """
#     # from evidently import ColumnMapping
#     # from evidently.report import Report
#     # from evidently.metrics import DataDriftTable, DataQualityTable

#     if report_type == "drift":
#         report = Report(metrics=[DataDriftPreset()])
#         report_name = "data_drift_report.html"
#     elif report_type == "quality":
#         report = Report(metrics=[DataQualityPreset()])
#         report_name = "data_quality_report.html"
#     else:
#         raise ValueError("Unsupported report type. Use 'drift' or 'quality'.")

#     # Run the report
#     #logger.info(f"Generating {report_type} report...")
#     report.run(reference_data=reference_df, current_data=current_df)

#     # Save the report locally in a temporary folder
#     local_folder_path = f"/tmp/{folder_path}"
#     os.makedirs(local_folder_path, exist_ok=True)
#     report_path = os.path.join(local_folder_path, report_name)
#     report.save_html(report_path)

#     #logger.info(f"{report_type.capitalize()} report saved locally at {report_path}")
#     print(f"{report_type.capitalize()} report saved locally at {report_path}")
#     return report_path, report_name


# # Upload Evidently Report to MinIO
# def upload_report_to_minio(report_path, report_name, folder_path="reports"):
#     """
#     Upload Evidently reports to MinIO.
#     """
#     with open(report_path, "rb") as report_file:
#         minio_client.put_object(
#             Bucket=MINIO_BUCKET,
#             Key=f"{folder_path}/{report_name}",
#             Body=report_file,
#             ContentType="text/html"
#         )
#     #logger.info(f"Uploaded Evidently report to MinIO: {folder_path}/{report_name}")
#     print(f"Uploaded Evidently report to MinIO: {folder_path}/{report_name}")
# Selenium-based Crawler
class StockPriceCrawler:
    def __init__(self):
        self.driver = None

    def reset_driver(self, remote_url="http://selenium:4444/wd/hub"):
        #logger.info("Initializing Selenium WebDriver")
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--headless')
        self.driver = webdriver.Remote(
            command_executor=remote_url,
            options=chrome_options
        )
        self.driver.implicitly_wait(10)
        #logger.info("Selenium WebDriver initialized successfully")

    def scrape_stock_prices(self, url):
        #logger.info(f"Scraping data from {url}")
        self.driver.get(url)
        time.sleep(5)

        final_row_data = None

        while True:
            # Get all rows on the current page
            rows = self.driver.find_elements(By.XPATH, '//tbody[@id="render-table-owner"]/tr')
            if rows:
                # Extract the last row's data
                last_row = rows[-1]
                cols = last_row.find_elements(By.TAG_NAME, 'td')
                final_row_data = [col.text.strip() for col in cols]

            # Check if a "Next" button exists and is enabled
            next_button = self.driver.find_elements(By.XPATH, '//a[contains(@class, "next")]')
            if next_button and "disabled" not in next_button[0].get_attribute("class"):
                next_button[0].click()  # Click "Next" to go to the next page
                time.sleep(3)  # Wait for the next page to load
            else:
                break  # Exit if no "Next" button or it's disabled

        columns = [
            "Ngày", "Giá Đóng cửa", "Điều chỉnh", "Thay đổi",
            "GD Khối lượng", "GD Giá trị (tỷ VNĐ)",
            "GD thỏa thuận Khối lượng", "GD thỏa thuận Giá trị (tỷ VNĐ)",
            "Giá Mở cửa", "Giá Cao nhất", "Giá Thấp nhất"
        ]
        # Return the final row as a DataFrame if data matches expected columns
        if final_row_data and len(final_row_data) == len(columns):
            return pd.DataFrame([final_row_data], columns=columns)
        else:
            print(f"Warning: No valid data found for URL {url}")
            return None

    def close_driver(self):
        if self.driver:
            #logger.info("Closing Selenium WebDriver")
            self.driver.quit()

def clean_numerical_columns(df, columns):
    """
    Ensure specified columns contain only numerical values. Replace invalid entries with NaN.
    """
    for column in columns:
        if column in df.columns:
            # Remove non-numeric characters and convert to numeric
            df[column] = (
                df[column]
                .astype(str)  # Ensure all values are strings before processing
                .str.replace(",", "", regex=False)  # Remove commas
                .str.replace(" ", "", regex=False)  # Remove spaces
                .str.replace(" ", "", regex=False)  # Remove non-breaking spaces
            )
            df[column] = pd.to_numeric(df[column], errors="coerce")  # Convert to numeric
    return df

def align_column_types(reference_df, current_df, columns):
    """
    Align the data types of specified columns between reference and current datasets.
    """
    for column in columns:
        if column in reference_df.columns and column in current_df.columns:
            reference_df[column] = pd.to_numeric(reference_df[column], errors="coerce")
            current_df[column] = pd.to_numeric(current_df[column], errors="coerce")
    return reference_df, current_df



def crawl_and_save(company_code, crawler, summary_logs):
    """
    Crawl stock data for the given company, upload it to MinIO, and create a symbol-specific reference dataset.
    """
    url = f"https://s.cafef.vn/Lich-su-giao-dich-{company_code}-1.chn"
    try:
        df = crawler.scrape_stock_prices(url)
    except Exception as e:
        print(f"Failed to crawl data for {company_code}: {str(e)}")
        summary_logs.append({
            "company_code": company_code,
            "url": url,
            "crawl_date": pd.Timestamp.now().isoformat(),
            "new_missing_percentage": None,
            "is_missing_increased": None,
            "is_important_column_missing": True,
            "crawl_status": "Failed"
        })
        return

    # Define folder path in MinIO
    folder_path = f"data/Ingestion/Close/CafeF/{company_code}"

    if df is not None and not df.empty:
        # Clean numerical columns
        numerical_columns = [
            "GD Giá trị (tỷ VNĐ)", "GD Khối lượng", "GD thỏa thuận Giá trị (tỷ VNĐ)",
            "GD thỏa thuận Khối lượng", "Giá Đóng cửa", "Giá Mở cửa", "Giá Cao nhất", "Giá Thấp nhất"
        ]
        df = clean_numerical_columns(df, numerical_columns)

        # Save stock data as <COMPANY_CODE>.csv
        csv_filename = f"{company_code}.csv"
        upload_to_minio(
            MINIO_BUCKET,
            folder_path,
            csv_filename,
            df,
            is_csv=True
        )

        # Initialize metrics
        metrics = {
            "company_code": company_code,
            "url": url,
            "crawl_date": pd.Timestamp.now().isoformat(),
            "new_missing_percentage": None,
            "is_missing_increased": None,
            "is_important_column_missing": False,
            "crawl_status": "Success"
        }

        # Check important columns for missing values
        important_columns = ["GD Khối lượng", "Giá Đóng cửa", "Giá Mở cửa"]
        missing_important_columns = df[important_columns].isnull().any().any()

        if missing_important_columns:
            metrics["is_important_column_missing"] = True
            print(f"Important columns have missing values for {company_code}.")

        # Check if reference_data.csv exists for this symbol
        reference_data_key = f"{folder_path}/reference_data.csv"
        try:
            minio_client.head_object(Bucket=MINIO_BUCKET, Key=reference_data_key)
            print(f"Reference data exists for {company_code}. Checking for missing values...")
            reference_data_path = f"/tmp/{company_code}_reference_data.csv"
            minio_client.download_file(MINIO_BUCKET, reference_data_key, reference_data_path)
            reference_df = pd.read_csv(reference_data_path)

            # Calculate missing percentages
            reference_missing_percentage = reference_df.isnull().mean().mean() * 100
            current_missing_percentage = df.isnull().mean().mean() * 100

            metrics["new_missing_percentage"] = current_missing_percentage
            metrics["is_missing_increased"] = current_missing_percentage > reference_missing_percentage

            # Log results
            print(
                f"{company_code}: Current missing: {current_missing_percentage:.2f}%, "
                f"Reference missing: {reference_missing_percentage:.2f}%, "
                f"Missing increased: {metrics['is_missing_increased']}"
            )

        except Exception as e:
            #logger.info(f"No reference data found for {company_code}. Creating new reference dataset.")
            print(f"No reference data found for {company_code}. Creating new reference dataset.")
            upload_to_minio(
                MINIO_BUCKET,
                folder_path,
                "reference_data.csv",
                df,
                is_csv=True
            )
            metrics["new_missing_percentage"] = df.isnull().mean().mean() * 100
            metrics["is_missing_increased"] = False  # No reference to compare

        # Append metrics to summary log
        summary_logs.append(metrics)

        #logger.info(f"Data for {company_code} saved successfully.")
        print(f"Data for {company_code} saved successfully.")
    else:
        #logger.warning(f"No data found for {company_code}.")
        print(f"No data found for {company_code}.")
        summary_logs.append({
            "company_code": company_code,
            "url": url,
            "crawl_date": pd.Timestamp.now().isoformat(),
            "new_missing_percentage": None,
            "is_missing_increased": None,
            "is_important_column_missing": True,
            "crawl_status": "No Data"
        })


if __name__ == "__main__":
    #logger.info("Starting stock price crawl process")

    # Upload List_company.csv to MinIO
    local_file_path = "List_company.csv"
    minio_folder_path = "data/Ingestion/Close/CafeF"
    upload_local_file_to_minio(MINIO_BUCKET, minio_folder_path, local_file_path)

    # Load company codes
    company_list = pd.read_csv(local_file_path)
    company_codes = company_list["Mã CK"].tolist()  # Adjust column name

    crawler = StockPriceCrawler()
    crawler.reset_driver()

    # Initialize summary log
    summary_logs = []

    try:
        for company_code in company_codes:
            #logger.info(f"Processing {company_code}...")
            crawl_and_save(company_code, crawler, summary_logs)
    finally:
        crawler.close_driver()

    # Save summary log to MinIO
    summary_df = pd.DataFrame(summary_logs)
    upload_to_minio(
        MINIO_BUCKET,
        "data/Ingestion/Close/CafeF",
        "summary_log.csv",
        summary_df,
        is_csv=True
    )

    #logger.info("Crawl process completed.")
    print("Crawl process completed.")
