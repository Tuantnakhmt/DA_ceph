# import os
# import pandas as pd
# import numpy as np
# import sys
# import json
# from tqdm import tqdm
# from bs4 import BeautifulSoup
# from newspaper import Article
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# from pymongo import MongoClient
# import urllib3

# sys.path.append('/app/')
# from VAR_GLOBAL_CONFIG_1 import *
# from Flow import PATH_env

# # MinIO Configuration
# import boto3
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# minio_client = boto3.client(
#     's3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# # MongoDB Configuration
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
# DB_NAME = "mlops_db"
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]

# # Utility Functions
# def download_from_minio(bucket, key, local_path):
#     with open(local_path, 'wb') as f:
#         minio_client.download_fileobj(bucket, key, f)
#     print(f"Downloaded {key} from MinIO to {local_path}")

# def upload_to_minio(local_path, bucket, key):
#     with open(local_path, 'rb') as f:
#         minio_client.upload_fileobj(f, bucket, key)
#     print(f"Uploaded {local_path} to {bucket}/{key}")

# def save_to_mongodb(df, collection_name):
#     try:
#         data = df.to_dict("records")
#         collection = db[collection_name]
#         collection.insert_many(data)
#         print(f"Data saved to MongoDB collection: {collection_name}")
#     except Exception as e:
#         print(f"Error saving data to MongoDB: {e}")

# def fetch_from_mongodb(collection_name, query={}):
#     try:
#         collection = db[collection_name]
#         data = pd.DataFrame(list(collection.find(query)))
#         print(f"Fetched {len(data)} records from MongoDB collection: {collection_name}")
#         return data
#     except Exception as e:
#         print(f"Error fetching data from MongoDB: {e}")
#         return pd.DataFrame()

# # Step 1: Load Symbol List
# symbol_file_key = "data/Ingestion/Close/CafeF/List_company.csv"
# local_symbol_file = "/tmp/List_company.csv"
# download_from_minio(MINIO_BUCKET, symbol_file_key, local_symbol_file)
# public_company_list = pd.read_csv(local_symbol_file)

# # Step 2: Crawl News from CafeF
# http = urllib3.PoolManager()
# all_datas = pd.DataFrame()

# def get_new_data(symbol, limit=4):
#     try:
#         r = http.request('GET', f'http://s.cafef.vn/Ajax/Events_RelatedNews_New.aspx?symbol={symbol}&floorID=0&configID=0&PageIndex=1&PageSize=10000&Type=2')
#         soup = BeautifulSoup(r.data, "html.parser")
#         data = soup.find("ul", {"class": "News_Title_Link"})
#         raw = data.find_all('li')[:limit]

#         data_dicts = []
#         for row in raw:
#             row_dict = {
#                 'newsdate': row.span.text.strip(),
#                 'title': row.a.text.strip(),
#                 'url': row.a['href'].strip(),
#                 'Symbol': symbol
#             }
#             data_dicts.append(row_dict)

#         return data_dicts
#     except Exception as e:
#         print(f"Error fetching data for {symbol}: {e}")
#         return []

# for ticker in tqdm(public_company_list["Mã CK"].values):
#     tickernews = pd.DataFrame(get_new_data(ticker, limit=4))
#     all_datas = pd.concat([all_datas, tickernews], ignore_index=True)

# all_datas["newsdate"] = pd.to_datetime(all_datas["newsdate"], format="%d/%m/%Y", errors="coerce")
# all_datas["Year"] = all_datas["newsdate"].dt.year
# all_datas["Quarter"] = all_datas["newsdate"].dt.quarter

# # Step 3: Fetch Financial Data
# financial_file_key = "data/Ingestion/MERGED_FINANCIAL_VALUE.csv"
# local_financial_file = "/tmp/MERGED_FINANCIAL_VALUE.csv"
# download_from_minio(MINIO_BUCKET, financial_file_key, local_financial_file)
# financial_data = pd.read_csv(local_financial_file)
# financial_data['Year'] = financial_data['Time']
# financial_data = financial_data.drop(columns=['Time'])

# # Step 4: Analyze News Details
# def get_details_from_url(df_to_get):
#     data_dicts = []
#     if df_to_get.empty:
#         print("The dataframe is empty. Exiting function.")
#         return data_dicts

#     for newsdate, url, symbol in tqdm(zip(df_to_get['newsdate'].values, df_to_get['url'].values, df_to_get['Symbol'].values)):
#         row_dict = {}
#         try:
#             article = Article(url, language='vi')
#             article.download()
#             article.parse()
#             article.nlp()

#             row_dict['newsdate'] = newsdate
#             row_dict['summary'] = article.summary
#             row_dict['keywords'] = article.keywords
#             row_dict['Symbol'] = symbol
#             row_dict['Year'] = pd.to_datetime(newsdate).year
#             data_dicts.append(row_dict)
#         except Exception as e:
#             print(f"Error for URL: {url} - {e}")
#             continue

#     return data_dicts

# news_detail = pd.DataFrame(get_details_from_url(all_datas))

# # Step 5: Sentiment Analysis
# analyzer = SentimentIntensityAnalyzer()

# def calculate_sentiment_vader(text):
#     if not text or pd.isna(text):
#         return 0
#     sentiment = analyzer.polarity_scores(text)
#     return sentiment["compound"]

# news_detail["keywords_text"] = news_detail["keywords"].apply(lambda x: " ".join(x) if isinstance(x, list) else "")
# news_detail["sentiment_score"] = news_detail["keywords_text"].progress_apply(calculate_sentiment_vader)

# sentiment_by_symbol_year = (
#     news_detail.groupby(["Symbol", "Year"])["sentiment_score"]
#     .mean()
#     .reset_index()
#     .rename(columns={"sentiment_score": "average_sentiment"})
# )

# # Step 6: Merge and Save Data
# merged_data = pd.merge(financial_data, sentiment_by_symbol_year, on=["Symbol", "Year"], how="left")
# local_output_file = "/tmp/Financial_with_Sentiment.csv"
# minio_output_key = "data/Ingestion/Financial_with_Sentiment.csv"

# merged_data.to_csv(local_output_file, index=False)
# upload_to_minio(local_output_file, MINIO_BUCKET, minio_output_key)

# # Step 7: Save to MongoDB
# save_to_mongodb(all_datas, "cafe_news")
# save_to_mongodb(news_detail, "news_details")

# # Example: Fetch crawled news
# crawled_data = fetch_from_mongodb("cafe_news")
# print(crawled_data.head())

# import pandas as pd
# import numpy as np
# import sys
# import os
# from pymongo import MongoClient
# import boto3
# import urllib3
# from bs4 import BeautifulSoup
# from tqdm import tqdm
# import logging
# from newspaper import Article
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# sys.path.append('/app/')
# from TransformData.VietNam.base.PATH_UPDATE import *
# from VAR_GLOBAL_CONFIG_1 import *

# # Logging setup
# logging.basicConfig(filename="crawl.log", level=logging.ERROR)

# # HTTP Manager
# http = urllib3.PoolManager()

# # MinIO Setup
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# minio_client = boto3.client(
#     's3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# # MongoDB Setup
# MONGO_URI = "mongodb://mongodb:27017"
# DB_NAME = "mlops_db"
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]

# # Download file from MinIO
# def download_from_minio(bucket, key, local_path):
#     """
#     Download a file from MinIO to a local path.
#     """
#     try:
#         with open(local_path, 'wb') as f:
#             minio_client.download_fileobj(bucket, key, f)
#         print(f"Downloaded {key} from MinIO to {local_path}")
#         return local_path
#     except Exception as e:
#         print(f"Error downloading {key} from MinIO: {e}")
#         return None

# # Save log to MinIO
# def save_log_to_minio(log_data):
#     local_log_path = "/tmp/crawl_news_log.csv"
#     log_data.to_csv(local_log_path, index=False)
#     minio_client.upload_file(local_log_path, MINIO_BUCKET, "data/Ingestion/crawl_news_log.csv")
#     print(f"Log saved to MinIO: data/Ingestion/crawl_news_log.csv")

# # Save financial sentiment data to MinIO
# def save_financial_to_minio(df):
#     local_path = "/tmp/Financial_with_Sentiment.csv"
#     df.to_csv(local_path, index=False)
#     minio_client.upload_file(local_path, MINIO_BUCKET, "data/Ingestion/Financial/Financial_with_Sentiment.csv")
#     print(f"Financial sentiment data saved to MinIO: data/Ingestion/Financial/Financial_with_Sentiment.csv")

# # Fetch news for a single symbol
# def get_new_data(symbol, limit=4):
#     try:
#         response = http.request('GET', f'http://s.cafef.vn/Ajax/Events_RelatedNews_New.aspx?symbol={symbol}&floorID=0&configID=0&PageIndex=1&PageSize=10000&Type=2')
#         soup = BeautifulSoup(response.data, "html.parser")
#         data = soup.find("ul", {"class": "News_Title_Link"})
#         raw = data.find_all('li')[:limit]

#         news = []
#         for row in raw:
#             news_item = {
#                 'newsdate': row.span.text.strip(),
#                 'title': row.a.text.strip(),
#                 'url': row.a['href'].strip(),
#                 'Symbol': symbol
#             }
#             news.append(news_item)
#         return news
#     except Exception as e:
#         logging.error(f"Error fetching data for {symbol}: {e}")
#         return []

# # Save crawled news to MongoDB
# def save_to_mongodb(df, collection_name):
#     try:
#         data = df.to_dict("records")
#         collection = db[collection_name]
#         collection.insert_many(data)
#         print(f"Data saved to MongoDB collection: {collection_name}")
#     except Exception as e:
#         print(f"Error saving data to MongoDB: {e}")

# # Main logic
# def main():
#     # Step 1: Fetch `List_company.csv` from MinIO
#     list_company_path = download_from_minio(
#         MINIO_BUCKET, "data/Ingestion/Close/CafeF/List_company.csv", "/tmp/List_company.csv"
#     )
#     if list_company_path is None:
#         print("Error: Could not download List_company.csv from MinIO.")
#         return

#     # Step 2: Fetch `MERGED_FINANCIAL_VALUE.csv` from MinIO
#     merged_financial_path = download_from_minio(
#         MINIO_BUCKET, "data/Ingestion/MERGED_FINANCIAL_VALUE.csv", "/tmp/MERGED_FINANCIAL_VALUE.csv"
#     )
#     if merged_financial_path is None:
#         print("Error: Could not download MERGED_FINANCIAL_VALUE.csv from MinIO.")
#         return

#     # Step 3: Read and process symbol list
#     symbols_df = pd.read_csv(list_company_path)
#     symbols = symbols_df["Mã CK"].values[:5]  # Test with the first 5 symbols

#     all_news = []
#     crawl_log = []

#     for symbol in tqdm(symbols, desc="Crawling news"):
#         news = get_new_data(symbol)
#         all_news.extend(news)
#         crawl_log.append({"Symbol": symbol, "NewsCount": len(news), "Status": "Success" if news else "Failed"})

#     # Step 4: Save news to MongoDB
#     news_df = pd.DataFrame(all_news)
#     save_to_mongodb(news_df, "cafe_news_1")

#     # Step 5: Save crawl log to MinIO
#     log_df = pd.DataFrame(crawl_log)
#     save_log_to_minio(log_df)

#     # Step 6: Sentiment analysis and merging
#     analyzer = SentimentIntensityAnalyzer()

#     # Add sentiment scores
#     news_df["sentiment_score"] = news_df["title"].apply(
#         lambda text: analyzer.polarity_scores(text)["compound"] if isinstance(text, str) else 0
#     )

#     # Average sentiment by Symbol and Year
#     news_df["newsdate"] = pd.to_datetime(news_df["newsdate"], format="%d/%m/%Y", errors="coerce")
#     news_df["Year"] = news_df["newsdate"].dt.year
#     sentiment_by_symbol_year = news_df.groupby(["Symbol", "Year"])["sentiment_score"].max().reset_index()

#     # Merge with financial data
#     financial_data = pd.read_csv(merged_financial_path)
#     financial_data['Year'] = financial_data['Time']
#     financial_data = financial_data.drop(columns=['Time'])
#     merged_data = pd.merge(financial_data, sentiment_by_symbol_year, on=["Symbol", "Year"], how="left")

#     # Step 7: Save merged data to MinIO
#     save_financial_to_minio(merged_data)

# if __name__ == "__main__":
#     main()

# #-----------------------
# import pandas as pd
# import os
# import sys
# import urllib3
# import json
# from bs4 import BeautifulSoup
# from newspaper import Article
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# from pymongo import MongoClient
# from tqdm import tqdm

# sys.path.append('/app/')

# from TransformData.VietNam.base.PATH_UPDATE import *
# from VAR_GLOBAL_CONFIG_1 import *
# from Flow.PATH_env import PATH_ENV

# # MinIO setup
# import boto3

# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# minio_client = boto3.client(
#     "s3",
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY,
# )

# # MongoDB setup
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
# DB_NAME = "mlops_db"
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]

# # Initialize components
# http = urllib3.PoolManager()
# analyzer = SentimentIntensityAnalyzer()

# # Download file from MinIO
# def download_from_minio(bucket, key, local_path):
#     """
#     Download a file from MinIO to a local path.
#     """
#     try:
#         with open(local_path, 'wb') as f:
#             minio_client.download_fileobj(bucket, key, f)
#         print(f"Downloaded {key} from MinIO to {local_path}")
#         return local_path
#     except Exception as e:
#         print(f"Error downloading {key} from MinIO: {e}")
#         return None

# # Fetch symbols from MinIO
# def fetch_symbols_from_minio():
#     local_file = "/tmp/List_company.csv"
#     minio_key = "data/Ingestion/Close/CafeF/List_company.csv"
#     minio_client.download_file(MINIO_BUCKET, minio_key, local_file)
#     symbols = pd.read_csv(local_file)["Mã CK"].tolist()
#     return symbols

# # Save to MongoDB
# def save_to_mongodb(df, collection_name):
#     try:
#         data = df.to_dict("records")
#         collection = db[collection_name]
#         collection.insert_many(data)
#         print(f"Data saved to MongoDB collection: {collection_name}")
#     except Exception as e:
#         print(f"Error saving to MongoDB: {e}")

# # Upload to MinIO
# def upload_to_minio(file_path, key):
#     try:
#         with open(file_path, "rb") as f:
#             minio_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=f)
#         print(f"Uploaded {key} to MinIO")
#     except Exception as e:
#         print(f"Error uploading to MinIO: {e}")

# # Crawl news data
# # def get_new_data(symbol, limit=4):
# def get_new_data(symbol):
#     try:
#         r = http.request(
#             "GET",
#             f"http://s.cafef.vn/Ajax/Events_RelatedNews_New.aspx?symbol={symbol}&floorID=0&configID=0&PageIndex=1&PageSize=10000&Type=2",
#         )
#         soup = BeautifulSoup(r.data, "html.parser")
#         data = soup.find("ul", {"class": "News_Title_Link"})
#         # raw = data.find_all("li")[:limit]
#         raw = data.find_all("li")
#         data_dicts = []
#         for row in raw:
#             row_dict = {
#                 "newsdate": row.span.text.strip(),
#                 "title": row.a.text.strip(),
#                 "Symbol": str(symbol),
#             }
#             raw_href = row.a["href"].strip()
#             if raw_href.startswith("/du-lieu/"):
#                 final_url = f"https://cafef.vn/{raw_href.lstrip('/')}"
#             elif not raw_href.startswith("http"):
#                 final_url = f"http://s.cafef.vn/{raw_href.lstrip('/')}"
#             else:
#                 final_url = raw_href
#             row_dict["url"] = final_url
#             data_dicts.append(row_dict)

#         return data_dicts

#     except Exception as e:
#         print(f"Error fetching data for {symbol}: {e}")
#         return []

# # Crawl details and calculate sentiment
# import nltk
# # nltk.download('punkt_tab')
# # def get_details_from_url(df_to_get):
# #     data_dicts = []
# #     for newsdate, url, symbol in tqdm(
# #         zip(df_to_get["newsdate"], df_to_get["url"], df_to_get["Symbol"])
# #     ):
# #         try:
# #             article = Article(url, language="vi")
# #             article.download()
# #             article.parse()
# #             article.nlp()
# #             row_dict = {
# #                 "newsdate": newsdate,
# #                 "summary": article.summary,
# #                 "keywords": article.keywords,
# #                 "Symbol": symbol,
# #             }
# #             data_dicts.append(row_dict)
# #         except Exception as e:
# #             print(f"Error for URL: {url} - {e}")
# #             continue
# #     return data_dicts

# #----------------------------------------------------------------------
# # def get_details_from_url(df_to_get):
# #     """
# #     Process the URLs to extract news details.
# #     """
# #     data_dicts = []
    
# #     # Check if the dataframe is empty
# #     if df_to_get.empty:
# #         print("The dataframe is empty. Exiting function.")
# #         return pd.DataFrame()

# #     print(f"Processing {len(df_to_get)} articles")

# #     for newsdate, url, symbol in tqdm(zip(df_to_get['newsdate'].values, df_to_get['url'].values, df_to_get['Symbol'].values)):
# #         row_dict = {}
# #         try:
# #             article = Article(url, language='vi')
# #             article.download()
# #             article.parse()
# #             article.nlp()
            
# #             row_dict['newsdate'] = newsdate
# #             row_dict['summary'] = article.summary
# #             row_dict['keywords'] = article.keywords
# #             row_dict['Symbol'] = symbol  # Add Symbol
            
# #             # Add 'Year' column based on 'newsdate'
# #             row_dict['Year'] = pd.to_datetime(newsdate, format="%Y-%m-%d", errors="coerce").year
            
# #             data_dicts.append(row_dict)
        
# #         except Exception as e:
# #             print(f"Error for URL: {url} - {e}")  # Print error for debugging
# #             continue  # Skip the problematic URL and continue to the next one
    
# #     print(f"Collected {len(data_dicts)} articles' details.")
# #     return pd.DataFrame(data_dicts)

# # # # Generate details DataFrame
# # # news_detail = get_details_from_url(all_datas)

# # # # Check if 'Year' column exists in news_detail
# # # if 'Year' not in news_detail.columns:
# # #     print("Error: 'Year' column not found in news_detail. Check the get_details_from_url function.")

# # # Process news and sentiment
# # def process_news(symbols):
# #     all_news = []
# #     logs = []

# #     for symbol in symbols:
# #         try:
# #             # news = get_new_data(symbol, limit=4)
# #             news = get_new_data(symbol)
# #             if news:
# #                 logs.append({"Symbol": symbol, "News Count": len(news), "Status": "Success"})
# #             else:  # If no news is fetched
# #                 logs.append({"Symbol": symbol, "News Count": 0, "Status": "No News Found"})
            
# #             # Add to the master news list
# #             all_news.extend(news)
        
# #         except Exception as e:
# #             logs.append({"Symbol": symbol, "News Count": 0, "Status": f"Failed - {str(e)}"})
# #             continue

# #     news_df = pd.DataFrame(all_news)
# #     news_df["newsdate"] = pd.to_datetime(
# #         news_df["newsdate"].str.split().str[0], format="%d/%m/%Y", errors="coerce"
# #     )
# #     news_df["Year"] = news_df["newsdate"].dt.year
# #     news_df["Quarter"] = news_df["newsdate"].dt.quarter


# #     sentiment_details = get_details_from_url(news_df)
# #     details_df = pd.DataFrame(sentiment_details)

# #     details_df["newsdate"] = pd.to_datetime(details_df["newsdate"], format="%Y-%m-%d", errors="coerce")

# #     details_df["Year"] = 2023
# #     details_df["sentiment_score"] = details_df["summary"].apply(
# #         lambda x: analyzer.polarity_scores(x)["compound"] if x else 0
# #     )

# #     logs_df = pd.DataFrame(logs)

# #     return news_df, details_df, logs_df
# #----------------------------------------------------------------------

# #----------------------------------------------------------------------
# def get_details_from_url(df_to_get):
#     """
#     Process the URLs to extract news articles.
#     """
#     data_dicts = []
    
#     # Check if the dataframe is empty
#     if df_to_get.empty:
#         print("The dataframe is empty. Exiting function.")
#         return pd.DataFrame()

#     print(f"Processing {len(df_to_get)} articles")

#     for newsdate, url, symbol in tqdm(zip(df_to_get['newsdate'].values, df_to_get['url'].values, df_to_get['Symbol'].values)):
#         row_dict = {}
#         try:
#             # Initialize the Article parser
#             article = Article(url, language='vi')
#             article.download()
#             article.parse()
            
#             # Collect details
#             row_dict['newsdate'] = newsdate
#             row_dict['content'] = article.text  # Entire article content
#             row_dict['title'] = article.title  # Article title
#             row_dict['url'] = url
#             row_dict['Symbol'] = symbol  # Add stock symbol
            
#             # Add 'Year' column based on 'newsdate'
#             row_dict['Year'] = pd.to_datetime(newsdate, format="%Y-%m-%d", errors="coerce").year
            
#             data_dicts.append(row_dict)
        
#         except Exception as e:
#             print(f"Error for URL: {url} - {e}")  # Print error for debugging
#             continue  # Skip the problematic URL and continue to the next one
    
#     print(f"Collected {len(data_dicts)} articles' details.")
#     return pd.DataFrame(data_dicts)

# def process_news(symbols):
#     all_news = []
#     logs = []

#     for symbol in symbols:
#         try:
#             news = get_new_data(symbol)
#             # news = get_new_data(symbol, limit=4)
#             if news:
#                 logs.append({"Symbol": symbol, "News Count": len(news), "Status": "Success"})
#             else:  # If no news is fetched
#                 logs.append({"Symbol": symbol, "News Count": 0, "Status": "No News Found"})
            
#             # Add to the master news list
#             all_news.extend(news)
        
#         except Exception as e:
#             logs.append({"Symbol": symbol, "News Count": 0, "Status": f"Failed - {str(e)}"})
#             continue

#     news_df = pd.DataFrame(all_news)
#     news_df["newsdate"] = pd.to_datetime(
#         news_df["newsdate"].str.split().str[0], format="%d/%m/%Y", errors="coerce"
#     )
#     news_df["Year"] = news_df["newsdate"].dt.year
#     news_df["Quarter"] = news_df["newsdate"].dt.quarter

#     # Fetch the full article details without summaries or sentiment
#     details_df = get_details_from_url(news_df)

#     logs_df = pd.DataFrame(logs)

#     return news_df, details_df, logs_df


# #----------------------------------------------------------------------
# # Fetch data from MongoDB
# def fetch_from_mongodb(collection_name, query={}):
#     """
#     Fetch data from a MongoDB collection.
#     """
#     try:
#         collection = db[collection_name]
#         data = pd.DataFrame(list(collection.find(query)))
#         print(f"Fetched {len(data)} records from MongoDB collection: {collection_name}")
#         return data
#     except Exception as e:
#         print(f"Error fetching data from MongoDB: {e}")
#         return pd.DataFrame()

# # Example Usage in Main Script
# if __name__ == "__main__":
#     symbols = fetch_symbols_from_minio() 
#     news_df, details_df, logs_df = process_news(symbols)


#     save_to_mongodb(news_df, "cafe_news_4")
#     save_to_mongodb(details_df, "news_details_4")

    # fetched_news = fetch_from_mongodb("cafe_news_2")
    # fetched_details = fetch_from_mongodb("news_details_2")


    # logs_file = "/tmp/crawl_logs.csv"
    # logs_df.to_csv(logs_file, index=False)
    # upload_to_minio(logs_file, "data/Ingestion/crawl_logs.csv")

    # merged_financial_path = download_from_minio(
    #     MINIO_BUCKET, "data/Ingestion/MERGED_FINANCIAL_VALUE.csv", "/tmp/MERGED_FINANCIAL_VALUE.csv"
    # )
    # if merged_financial_path is None:
    #     print("Error: Could not download MERGED_FINANCIAL_VALUE.csv from MinIO.")


    # financial_data = pd.read_csv("/tmp/MERGED_FINANCIAL_VALUE.csv")
    # financial_data['Year']=financial_data['Time']
    # sentiment_data = (
    #     details_df.groupby(["Symbol", "Year"])
    #     .agg({"sentiment_score": "max"})
    #     .reset_index()
    # )

    # merged_data = pd.merge(
    #     financial_data, sentiment_data, on=["Symbol", "Year"], how="left"
    # )


    # sentiment_file = "/tmp/Financial_with_Sentiment.csv"
    # merged_data.to_csv(sentiment_file, index=False)
    # upload_to_minio(sentiment_file, "data/Ingestion/Financial_with_Sentiment.csv")

    # fetched_merged = fetch_from_mongodb("news_details", {"Symbol": {"$in": symbols}})
    # print(f"Fetched merged data:\n{fetched_merged.head()}")

import os
from pymongo import MongoClient

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
DB_NAME = "mlops_db"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def get_average_record_size(collection_name):
    """
    Calculate the average size of each record in the collection.
    """
    try:
        stats = db.command("collstats", collection_name)
        total_size = stats.get("size", 0)  # Total size in bytes
        record_count = stats.get("count", 0)  # Total number of documents

        if record_count == 0:
            print(f"Collection '{collection_name}' has no records.")
            return None
        
        avg_record_size = total_size / record_count  # Average size per record in bytes
        print(f"Collection '{collection_name}':")
        print(f" - Total size: {total_size / (1024 ** 2):.2f} MB")
        print(f" - Total records: {record_count}")
        print(f" - Average record size: {avg_record_size:.2f} bytes ({avg_record_size / 1024:.2f} KB)")
        return avg_record_size
    except Exception as e:
        print(f"Error fetching stats for collection '{collection_name}': {e}")
        return None

# Example usage
if __name__ == "__main__":
    collections = ["cafe_news_4", "news_details_4"]
    for collection_name in collections:
        get_average_record_size(collection_name)
