# import os
# import json
# import pandas as pd
# import numpy as np
# import boto3
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from bs4 import BeautifulSoup

# # MinIO Configuration
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# # Initialize MinIO Client
# minio_client = boto3.client(
#     's3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY
# )

# def ensure_bucket(bucket_name):
#     try:
#         minio_client.head_bucket(Bucket=bucket_name)
#         # print(f"Bucket '{bucket_name}' exists.")
#     except Exception:
#         # print(f"Creating bucket '{bucket_name}'.")
#         minio_client.create_bucket(Bucket=bucket_name)

# ensure_bucket(MINIO_BUCKET)

# class StockPriceCrawler:
#     def __init__(self):
#         self.driver = None
#         self.link = "https://s.cafef.vn/bao-cao-tai-chinh/SYMBOL/incsta/2024/0/0/0/bao-cao-tai-chinh-.chn"

#     def reset_driver(self, remote_url="http://selenium:4444/wd/hub"):
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

#     def setup_link(self, symbol):
#         self.link = self.link.replace("SYMBOL", symbol)

#     def request_link(self, link):
#         self.driver.get(link)

#     def clickBalance(self):
#         self.driver.find_element(By.ID, "aNhom1").click()

#     def clickPerious(self):
#         '''
#         Click vào nút lùi
#         '''
#         self.driver.find_element(By.XPATH, '//*[@id="tblGridData"]/tbody/tr/td[1]/div/a[1]').click()

#     def getData(self, times):
#         '''
#         Lấy dữ liệu
#         Input: số lần lấy dữ liệu
#         Output: bảng chứa dữ liệu
#         '''
#         df = {}
#         while times != 0:
#             times -= 1
#             df1 = self.getTable()
#             df[df1.columns[1]] = df1.to_dict('records')
#             self.clickPerious()
#         return df

#     def getTable(self):
#         '''
#         Lấy dữ liệu từ bảng
#         Input: None
#         Output: bảng chứa dữ liệu
#         '''
#         page_sourse = self.driver.page_source
#         soup = BeautifulSoup(page_sourse, "html.parser")
#         table = soup.find('table', {'id': 'tblGridData'})
#         header = pd.read_html(str(table), displayed_only=False)
#         time = np.array_str(header[0][4].values)
#         table = soup.find('table', {'id': 'tableContent'})
#         financial = pd.read_html(str(table), displayed_only=False)
#         df = financial[0][[0, 4]]
#         df = df.dropna(subset=[0])
#         df = df.rename(columns={0: "field",
#                                 4: time})
#         return df

#     def get_Balance(self, symbol):
#         self.setup_link(symbol)
#         self.request_link(self.link)
#         self.clickBalance()
#         return self.getData(5)

# def upload_to_minio(bucket, folder_path, filename, data):
#     key = f"{folder_path}/{filename}"
#     data = json.dumps(data, ensure_ascii=False).encode('utf-8')
#     minio_client.put_object(
#         Bucket=bucket,
#         Key=key,
#         Body=data,
#         ContentType="application/json"
#     )
#     # print(f"Uploaded {key} to MinIO.")

# def fetch_symbols_from_minio():
#     local_file = "/tmp/List_company.csv"
#     minio_key = "data/Ingestion/Close/CafeF/List_company.csv"
#     minio_client.download_file(MINIO_BUCKET, minio_key, local_file)
#     symbols = pd.read_csv(local_file)["Mã CK"].tolist()
#     return symbols

# if __name__ == "__main__":
#     crawler = StockPriceCrawler()
#     crawler.reset_driver()

#     symbols = fetch_symbols_from_minio()[:2]

#     for symbol in symbols:
        
#         balance_data = crawler.get_Balance(symbol)

#         # Upload to MinIO
#         folder_path = "data/Ingestion/Financial/CafeF/Year/BalanceSheet"
#         filename = f"{symbol}_2024.json"
#         upload_to_minio(MINIO_BUCKET, folder_path, filename, balance_data)

#     print("Thử nghiệm cho 2 mã thành công")



import os
import json
import pandas as pd
import numpy as np
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import time

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# VietStock Login Credentials
VIETSTOCK_URL = "https://finance.vietstock.vn/"
USER = "anhtuan962002@gmail.com"
PASSWORD = "tuantran96"

# Initialize MinIO Client
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def ensure_bucket(bucket_name):
    try:
        minio_client.head_bucket(Bucket=bucket_name)
    except Exception:
        minio_client.create_bucket(Bucket=bucket_name)

ensure_bucket(MINIO_BUCKET)

class VietStockCrawler:
    def __init__(self):
        self.driver = None
        self.VS = VIETSTOCK_URL
        self.user = USER
        self.password = PASSWORD

    def reset_driver(self, remote_url="http://selenium:4444/wd/hub"):
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

    def login_VS(self):
        '''
        Đăng nhập VietStock
        Input:
        None
        Output:
        None
        '''
        self.driver.get(self.VS)
        self.driver.maximize_window()
        try:
            self.driver.find_element(By.ID, 'btn-request-call-login').click()
            self.driver.find_element(By.ID, 'txtEmailLogin').send_keys(self.user)
            self.driver.find_element(By.ID, 'txtPassword').send_keys(self.password)
            self.driver.find_element(By.ID, 'btnLoginAccount').click()
        finally:
            time.sleep(10)

    # def request_link(self, link):
    #     self.driver.get(link)

    # def click_select(self, element_id, value):
    #     select_element = self.driver.find_element(By.ID, element_id)
    #     for option in select_element.find_elements(By.TAG_NAME, 'option'):
    #         if option.get_attribute("value") == value:
    #             option.click()
    #             break

    # def click_something_by_xpath(self, something):
    #     '''
    #     Click vào element theo xpath
    #     Input:
    #     something: xpath
    #     Output:
    #     None
    #     '''
    #     try:
    #         element = WebDriverWait(self.driver, 10).until(
    #             EC.presence_of_element_located((By.XPATH, something))
    #         )
    #         element.click()
    #     except:
    #         pass

    # def BalanceSheet(self, symbol, PeriodType="NAM"):
    #     link_balance = f"https://finance.vietstock.vn/{symbol}/tai-chinh.htm?tab=CDKT"
    #     return self.table_lake(link_balance, PeriodType, True)

    # def table_lake(self, link, PeriodType, *arg):
    #     self.request_link(link)
    #     self.click_to_all_year(PeriodType, *arg)
    #     data = self.getTable()
    #     return data

    # def click_to_all_year(self, PeriodType, *arg):
    #     try:
    #         self.click_select("period", "2")
    #         time.sleep(0.5)
    #         self.click_select("UnitDong", "1000")
    #         time.sleep(0.5)
    #         self.click_select("PeriodType", str(PeriodType))
    #         time.sleep(0.5)
    #         # if arg[0] != False:
    #         try:
    #             self.click_something_by_xpath('//*[@id="expand-overall-CDKT"]')
    #             time.sleep(10)
    #         except:
    #             pass
    #     except:
    #         pass

    # def getTable(self):
    #     page_source = self.driver.page_source
    #     soup = BeautifulSoup(page_source, "html.parser")
    #     list_table = soup.find_all("table", {"class": "table table-hover"})
    #     try:
    #         data = pd.read_html(str(list_table))
    #         try:
    #             data = pd.concat([data[0], data[1]])
    #         except:
    #             data = data[0]
    #     except:
    #         data = pd.DataFrame({'Nothing': []})
    #     return data
    def request_link(self, link):
        self.driver.get(link)

    def click_select(self, name, value):
        '''
        Chọn giá trị trong select 
        Input: 
        name: tên select 
        value: giá trị cần chọn 
        Output: 
        None'''
        select = Select(self.driver.find_element(By.NAME, name))
        select.select_by_value(value)

    def click_something_by_xpath(self, something):
        '''
        Click vào element theo xpath
        Input:
        something: xpath
        Output:
        None
        '''
        try:
            element = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, something))
            )
            element.click()
        except:
            pass

    def BalanceSheet(self, symbol, PeriodType="NAM"):
        link_balance = f"https://finance.vietstock.vn/{symbol}/tai-chinh.htm?tab=CDKT"
        return self.table_lake(link_balance, PeriodType, True)

    def table_lake(self, link, PeriodType, *arg):
        self.request_link(link)
        self.click_to_all_year(PeriodType, *arg)
        data = self.getTable()
        return data

    def click_to_expand_data(self):

        try:
            # Use JavaScript execution to click
            element = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.ID, "expand-overall-CDKT"))
            )
            self.driver.execute_script("arguments[0].click();", element)
            time.sleep(1)  # Adjust sleep to wait for data to load
        except Exception as e:
            print(f"Error while expanding data: {e}")


    def click_to_all_year(self, PeriodType, *arg):
        '''
        Chọn tất cả các năm
        Output: DataFrame
        '''
        try:
            try:
                self.click_select("period", "2")
                time.sleep(0.5)
                self.click_select("UnitDong", "1000")
                time.sleep(0.5)
                self.click_select("PeriodType", PeriodType)
                time.sleep(0.5)
            except: pass
            # if arg[0] != False:
            #     try:
            #         self.click_something_by_xpath('//*[@id="expand-overall-CDKT"]')
            #         time.sleep(5)
            #         self.click_something_by_xpath('//*[@id="expand-overall-CDKT"]')
            #         time.sleep(5)
            #     except: pass
            try:
                self.click_to_expand_data()
                time.sleep(5)
                print("Nhấn l1")
                self.click_to_expand_data()
                time.sleep(5)
                print("Nhấn l2")
            except: pass
        except:
            pass

    def getTable(self):
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        list_table = soup.find_all("table", {"class": "table table-hover"})
        try:
            data = pd.read_html(str(list_table))
            try:
                data = pd.concat([data[0], data[1]])
            except:
                data = data[0]
        except:
            data = pd.DataFrame({'Nothing': []})
        return data

def upload_to_minio(bucket, folder_path, filename, data):
    key = f"{folder_path}/{filename}"
    data.to_csv("/tmp/temp_data.csv", index=False)
    with open("/tmp/temp_data.csv", "rb") as file_data:
        minio_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_data,
            ContentType="text/csv"
        )
    print(f"Uploaded {key} to MinIO.")

def fetch_symbols_from_minio():
    local_file = "/tmp/List_company.csv"
    minio_key = "data/Ingestion/Close/CafeF/List_company.csv"
    minio_client.download_file(MINIO_BUCKET, minio_key, local_file)
    symbols = pd.read_csv(local_file)["Mã CK"].tolist()
    return symbols

if __name__ == "__main__":
    crawler = VietStockCrawler()
    crawler.reset_driver()
    crawler.login_VS()

    # symbols = fetch_symbols_from_minio()[:2]
    symbols = fetch_symbols_from_minio()
    for symbol in symbols:
        balance_data = crawler.BalanceSheet(symbol)

        folder_path = "data/Ingestion/Financial/VietStock/Year/BalanceSheet"
        # filename = f"{symbol}_2024.csv"
        filename = f"{symbol}.csv"
        upload_to_minio(MINIO_BUCKET, folder_path, filename, balance_data)

    print("ok")

