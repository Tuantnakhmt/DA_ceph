# import pandas as pd
# import sys
# # sys.path.append(r'D:\DATN')
# # sys.path.append(r'D:\DATN\TransformData\VietNam')
# sys.path.append('/app/')
# sys.path.append('/app/TransformData/VietNam')

# from TransformData.VietNam.base import Compare
# from TransformData.VietNam.base.Financial import CafeF,VietStock
# from TransformData.VietNam.base.PATH_UPDATE import *
# from TransformData.VietNam.base.Setup import *
# Type_Time = "Year"
# # CafeF
# # SYMBOL = ["SFI"]
# df_check_list = pd.DataFrame()
# def transform(symbol,field):
#     '''
#     Chuyển đổi data từ CafeF và VietStock\n
#     Input:\n
#     symbol: mã cổ phiếu\n
#     field: tên loại thời gian \n
#     Output:\n
#     df_check_list: DataFrame
#     '''
#     global df_check_list
#     CF = CafeF(dict_path_cf)
#     cf = CF.run(symbol,field)
#     VS = VietStock(dict_path_vs)
#     vs = VS.run(symbol,field)
#     df = pd.DataFrame({ "Symbol": [symbol],
#                         "Type_Time": [field],
#                         "CafeF": [cf],
#                         "VietStock":[vs]})
#     df_check_list = pd.concat([df_check_list,df],ignore_index=True)
#     return df_check_list
# # transform("ABS","Year")

# for symbol in SYMBOL:
#     # transform(symbol,"Quarter")
#     transform(symbol,"Year")

# df_check_list.to_excel(FU.joinPath(FU.PATH_COMPARE,f"Financial_{Type_Time}_CheckList.xlsx"))

# def setup_Feature(type_time):
#     '''
#     Lấy dữ liệu cần lấy \n
#     Input: \n
#     type_time: loại thời gian \n
#     Output: \n
#     data_field: dữ liệu cần lấy \n'''
#     if type_time == "Year":
#         sheet_name = "Total"
#     else:
#         sheet_name = "Quarter"
#     data_field = pd.read_excel(f'{dict_path_vs["Feature"]}/Feature_Standard_Library.xlsx',sheet_name=sheet_name)
#     data_field = data_field.rename(columns={"column":"Feature"})
#     # print(data_field)
#     return data_field

# # def RunCompare(type_time):
# #     '''
# #     Chạy so sánh \n
# #     Input: \n
# #     type_time: loại thời gian \n
# #     Output: None \n
# #     '''
# #     can_t_compare = []
# #     data_field = setup_Feature(type_time)
# #     for symbol in SYMBOL:
# #         try:
# #             C = Compare.CompareFinancial(symbol,PATH_FT,type_time,data_field)
# #             data = C.get_field("CF","VS")
# #             data.to_csv(FU.joinPath(FU.PATH_COMPARE,"Financial",type_time,f"{symbol}.csv"),index=False)
# #         except:
# #             can_t_compare.append(symbol)
# #     pd.DataFrame({"Error_Compare":can_t_compare}).to_excel(FU.joinPath(FU.PATH_COMPARE,"Error",f"{type_time}.xlsx"),index=False)        

# def RunCompare(type_time):
#     '''
#     Run the comparison \n
#     Input: \n
#     type_time: type of time (e.g., "Year") \n
#     Output: None \n
#     '''
#     can_t_compare = []
#     data_field = setup_Feature(type_time)

#     for symbol in SYMBOL:
#         print(f"Processing symbol: {symbol}")
#         try:
#             C = Compare.CompareFinancial(symbol, PATH_FT, type_time, data_field)
#             data = C.get_field("CF", "VS")
#             print(f"Data for {symbol}:\n{data.head()}")
            
#             # Save data to CSV
#             file_path = FU.joinPath(FU.PATH_COMPARE, "Financial", type_time, f"{symbol}.csv")
#             data.to_csv(file_path, index=False)
#             print(f"Saved data for symbol {symbol} to {file_path}")
#         except Exception as e:
#             print(f"Error processing symbol {symbol}: {e}")
#             can_t_compare.append(symbol)

#     # Save errors to a separate file
#     error_file_path = FU.joinPath(FU.PATH_COMPARE, "Error", f"{type_time}.xlsx")
#     pd.DataFrame({"Error_Compare": can_t_compare}).to_excel(error_file_path, index=False)
#     print(f"Symbols that couldn't be compared saved to {error_file_path}")
    
# # RunCompare("Quarter")
# RunCompare("Year")

import pandas as pd
import os
import boto3
import sys

# Add paths to the system path
sys.path.append('/app/')
sys.path.append('/app/TransformData/VietNam')

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawled-data")

# Initialize MinIO client
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

Type_Time = "Year"

class CompareFinancial:
    """
    Compare financial data from CafeF and VietStock accessed directly from MinIO.
    """
    def __init__(self, symbol, type_time, data_field):
        self.symbol = symbol
        self.type_time = type_time
        self.data_field = data_field["Feature"].to_frame()
        self.dict_data = {
            "CF": {"path": f"data/Ingestion/Financial/CafeF/F3/{type_time}/", "company": pd.DataFrame()},
            "VS": {"path": f"data/Ingestion/Financial/VietStock/F3/{type_time}/", "company": pd.DataFrame()},
        }
        self.get_data()

    # def get_data(self):
    #     for key in self.dict_data.keys():
    #         print(f"Processing data for key: {key}")
    #         file_path = f"{self.dict_data[key]['path']}{self.symbol}.csv"
    #         try:
    #             response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_path)
    #             df = pd.read_csv(response["Body"])
    #             for column in df.columns[1:]:
    #                 df[column] = df[column].astype(float)
    #             if "2023" not in df.columns:
    #                 df["2023"] = 0.0
    #             self.dict_data[key]["company"] = df
    #         except Exception as e:
    #             print(f"Error processing data for key: {key}, symbol: {self.symbol}, file: {file_path}: {e}")
    #             self.dict_data[key]["company"] = self.data_field.copy()
    #             self.dict_data[key]["company"]["2023"] = 0.0

    def get_data(self):
        for key in self.dict_data.keys():
            print(f"Processing data for key: {key}")
            file_path = f"{self.dict_data[key]['path']}{self.symbol}.csv"

            try:
                response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=file_path)
                df = pd.read_csv(response["Body"])

                df.columns = df.columns.str.strip()  # Loai bo khoang trang
                df["Feature"] = df["Feature"].str.strip()  # Match name

                for column in df.columns[1:]:
                    df[column] = pd.to_numeric(df[column], errors="coerce")

                if "2023" not in df.columns:
                    print(f"Year 2023 column is missing for key: {key}. Creating it and filling with zeros.")
                    df["2023"] = 0.0

                print(f"Loaded data for {key}:\n{df.head()}")

            except Exception as e:
                print(f"Error processing data for key: {key}, symbol: {self.symbol}, file: {file_path}: {e}")
                df = self.data_field.copy()
                df["2023"] = 0.0  

            self.data_field["Feature"] = self.data_field["Feature"].str.strip()  # Dam bao match name
            self.data_field = self.data_field.drop_duplicates(subset=["Feature"])
            df = df.drop_duplicates(subset=["Feature"])
            df = pd.merge(self.data_field, df, on=["Feature"], how="left")
            df.fillna(0, inplace=True)  

            print(f"Merged data for {key}:\n{df.head()}")
            

            self.dict_data[key]["company"] = df


    def get_time(self, data):
        """
        Chi lay nam 2023.
        """
        all_years = data.columns[1:]  
        filtered_years = [year for year in all_years if year == "2023"]
        print(f"Filtered years for comparison: {filtered_years}")

        return filtered_years

    def compare_2_block(self, val1, val2, scale1, scale2, feature):
        """
        Input:
        val1, val2: Values to compare
        scale1, scale2: Scaling factors
        feature: Feature name (optional for context)

        Output:
        Comparison result (e.g., True/False, or difference)
        """
        return abs(val1 * scale1 - val2 * scale2) < 1e-5

    # def get_field(self, key_1, key_2):
    #     """
    #     Compare data from two sources.

    #     Input:
    #     key_1: Source 1 key (e.g., "CF")
    #     key_2: Source 2 key (e.g., "VS")

    #     Output:
    #     DataFrame with comparison results
    #     """
    #     df = pd.merge(self.dict_data[key_1]["company"], self.dict_data[key_2]["company"], on=["Feature"], how="inner")
    #     print(f"Merged data for comparison ({key_1} vs {key_2}):\n{df.head()}")

    #     list_year = self.get_time(self.dict_data[key_1]["company"])

    #     # for year in list_year:
    #     #     print(f"Comparing data for year: {year}")
    #     #     df[f"Compare_{year}"] = df.apply(
    #     #         lambda row: self.compare_2_block(row[f"{year}_x"], row[f"{year}_y"],
    #     #                                          self.dict_data[key_1]["money"], self.dict_data[key_2]["money"],
    #     #                                          row["Feature"]),
    #     #         axis=1
    #     #     )
    #     for year in list_year:
    #         print(f"Comparing data for year: {year}")
    #         df["Compare"] = df.apply(lambda row: self.compare_2_block(
    #             row[f"{year}_x"], row[f"{year}_y"], s_a, s_b, row["Feature"]
    #         ), axis=1)

    #     print(f"Final comparison data:\n{df.head()}")
    #     print(f"Final comparison data:\n{df.head()}")
    #     return df
    def get_field(self, key_1, key_2):
        '''
        Compare data from two sources.

        Input:
        key_1: Source 1 key (e.g., "CF")
        key_2: Source 2 key (e.g., "VS")

        Output:
        DataFrame with comparison results
        '''
        df = pd.merge(self.dict_data[key_1]["company"], self.dict_data[key_2]["company"], on=["Feature"], how="inner")
        print(f"Merged data for comparison ({key_1} vs {key_2}):\n{df.head()}")
        # print("df get field")
        list_year = self.get_time(self.dict_data[key_1]["company"])

        for year in list_year:
            print(f"Comparing data for year: {year}")
            # Add a 'Compare' column with values as `0` or `1`
            df["Compare"] = df.apply(
                lambda row: 1 if abs(row[f"{year}_x"] - row[f"{year}_y"]) < 1e-5 else 0, axis=1
            )

        df = df[["Feature"] + [f"{year}_x" for year in list_year] + [f"{year}_y" for year in list_year] + ["Compare"]]
        print(f"Final comparison data:\n{df.head()}")
        return df


from io import BytesIO

def setup_Feature(type_time):
    '''
    Fetch the feature library from MinIO
    Input: 
    type_time: type of time (e.g., 'Year')
    Output: 
    data_field: Data fields to retrieve as a DataFrame
    '''
    try:
        # Download feature_lib.xlsx from MinIO
        feature_lib_key = "data/Ingestion/Compare/feature_lib.xlsx"
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=feature_lib_key)
        
        # Read the stream into a BytesIO buffer
        excel_data = BytesIO(response['Body'].read())
        
        # Load the Excel file with pandas
        feature_lib = pd.read_excel(excel_data, sheet_name="Total")
        feature_lib = feature_lib.rename(columns={"column": "Feature"})
        return feature_lib
    except Exception as e:
        print(f"Error loading feature library from MinIO: {e}")
        raise


def save_to_minio(bucket, key, dataframe):
    '''
    Save a DataFrame as a CSV file to MinIO.
    '''
    try:
        # Save DataFrame to a temporary CSV file
        temp_file = "/tmp/temp_output.csv"
        dataframe.to_csv(temp_file, index=False)

        # Upload the file to MinIO
        with open(temp_file, "rb") as file_data:
            minio_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=file_data,
                ContentType="text/csv"
            )
        print(f"Saved data to MinIO at {key}")
    except Exception as e:
        print(f"Error saving to MinIO: {e}")
        raise

def get_symbols_from_minio():
    '''
    Load the list of company codes from MinIO.
    '''
    try:
        local_file_path = "List_company.csv"
        minio_folder_path = "data/Ingestion/Close/CafeF"
        minio_key = f"{minio_folder_path}/{local_file_path}"

        # Download the file from MinIO
        response = minio_client.get_object(Bucket=MINIO_BUCKET, Key=minio_key)
        with open(local_file_path, "wb") as file_data:
            file_data.write(response['Body'].read())

        company_list = pd.read_csv(local_file_path)
        # company_list = pd.read_csv(local_file_path).head(30)
        # print(company_list)
        company_codes = company_list["Mã CK"].tolist()  
        print(f"Loading symbols")
        return company_codes
    except Exception as e:
        print(f"Error loading company list from MinIO: {e}")
        raise

def RunCompare(type_time):
    '''
    Run the comparison 
    Input: 
    type_time: type of time (e.g., "Year") 
    Output: None 
    '''
    data_field = setup_Feature(type_time)
    SYMBOL = get_symbols_from_minio()

    log_data = []

    for symbol in SYMBOL:
        print(f"Processing symbol: {symbol}")
        try:
            C = CompareFinancial(symbol, type_time, data_field)
            data = C.get_field("CF", "VS")
            file_key = f"data/Ingestion/Compare/Financial/{type_time}/{symbol}.csv"
            save_to_minio(MINIO_BUCKET, file_key, data)
            log_data.append({"Symbol": symbol, "Status": "Saved"})
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")
            log_data.append({"Symbol": symbol, "Status": "Failed", "Error": str(e)})

    log_df = pd.DataFrame(log_data)
    log_key = f"data/Ingestion/Compare/Financial/log_compare.csv"
    save_to_minio(MINIO_BUCKET, log_key, log_df)
    print(f"Log file saved to MinIO at {log_key}")

RunCompare("Year")


