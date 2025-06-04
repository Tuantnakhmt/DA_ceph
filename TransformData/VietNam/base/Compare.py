import math
import pandas as pd
import sys
import numpy as np
sys.path.append('/app/')

from VAR_GLOBAL_CONFIG import *


def check_dau(a):
    if a >=0:
        return 1
    else:
        return -1


class Compare():
    '''
    So sánh đối chiếu các nguồn dữ liệu \n
    '''
    def __init__(self) -> None:
        pass

    def CompareNumber(self,a,b):
        '''
        So sánh 2 số \n
        Input: a: số thứ nhất \n
        b: số thứ hai \n
        Output: 
        1: a = b \n
        0: a != b \n
        2: a hoặc b là NaN \n
        N: a và b là NaN \n'''
        if math.isnan(a) and math.isnan(b):
            return "N"
        if math.isnan(a) or math.isnan(b):
            return "2"
        if round(a)-round(b) == 0:
            return "1"
        else:
            return "0"
    def compare_2_block(self,a,b,s_a=1,s_b=1,field=""):
        '''
        So sánh 2 số \n
        Input: \n
        a: số thứ nhất \n
        b: số thứ hai \n
        s_a: đơn vị tiền của a \n
        s_b: đơn vị tiền của b \n
        field: tên cột \n
        Output: \n
        1: a = b \n
        0: a != b \n
        2: a hoặc b là NaN \n
        N: a và b là NaN \n
        '''
        if math.isnan(a) and math.isnan(b):
            return "N"
        if math.isnan(a) or math.isnan(b):
            return "2"
        if field in ['Basic earnings per share','Diluted earnings per share']:
            s_a = 1
            s_b = 1
        dau_a = check_dau(a)
        dau_b = check_dau(b)
        a = abs(a)
        b = abs(b)
        x,y = a,b
        x = a/s_a+0.0000001
        y = b/s_b+0.0000001
        x = dau_a*x
        y = dau_b*y
        if round(x)-round(y) == 0:
            return "1"
        else:
            return "0"
    def compare_2_string(self,a,b):
        '''
        So sánh 2 chuỗi \n
        Input: \n
        a: chuỗi thứ nhất \n
        b: chuỗi thứ hai \n
        Output: \n
        1: a = b \n
        0: a != b \n
        2: a hoặc b là NaN \n
        N: a và b là NaN \n

        '''
        # if math.isnan(a) and math.isnan(b):
        #     return "N"
        # if math.isnan(a) or math.isnan(b):
        #     return "2"
        if a == b:
            return "1"
        else:
            return "0"

class CompareFinancial(Compare):
    '''
    So sánh đối chiếu các nguồn dữ liệu tài chính \n
    '''
    def __init__(self,symbol,path_,type_time,data_field) -> None:
        '''
        Input: \n
        symbol: mã cổ phiếu \n
        path_: đường dẫn \n
        type_time: loại thời gian \n
        data_field: dữ liệu cần lấy \n
        Output: None \n
        '''
        self.symbol = symbol
        self.path_main = path_
        self.type_time = type_time
        self.dict_data={
            "CF":{"path":[self.path_main+f"/Financial/CafeF/F3/{type_time}/"],"company":pd.DataFrame({}),"money":1},
            "VS":{"path":[self.path_main+f"/Financial/VietStock/F3/{type_time}/"],"company":pd.DataFrame({}),"money":1}
        }
        self.getDataField(data_field)
        self.getData()


    def getDataField(self,data_field):
        '''
        Lấy dữ liệu cần lấy \n
        Input: data_field: dữ liệu cần lấy \n
        Output: None \n'''
        self.data_field = data_field[["Feature"]]

    def getData(self):
        '''
        Lấy dữ liệu từ các nguồn \n
        Input: None \n
        Output: None \n
        '''
        for key in self.dict_data.keys():
            print(f"Processing data for key: {key}")
            print(f"Expected path: {self.dict_data[key]['path'][0]}")
            try:
                # Load the CSV file
                df = pd.read_csv("{}/{}.csv".format(self.dict_data[key]["path"][0], self.symbol))
                print(f"Loaded data for {key}:\n{df.head()}")

                # Convert columns to float starting from the second column
                for column in df.columns[1:]:
                    df[column] = df[column].astype(float)

                # Add missing year column if necessary
                if "2023" not in df.columns:
                    print(f"Year 2023 column is missing for key: {key}. Creating it and filling with zeros.")
                    df["2023"] = 0.0

                print(f"After adding missing columns (if any):\n{df.head()}")
            except FileNotFoundError as e:
                print(f"File not found for key: {key}, path: {self.dict_data[key]['path'][0]}, symbol: {self.symbol}")
                df = self.data_field.copy()
                df["2023"] = 0.0  # Add 2023 column with zeros
            except Exception as e:
                print(f"Error processing data for key: {key}, symbol: {self.symbol}: {e}")
                df = self.data_field.copy()
                df["2023"] = 0.0  # Add 2023 column with zeros

            # Merge the data field with the loaded or default data
            df = pd.merge(self.data_field, df, on=["Feature"], how="left")
            print(f"Merged data for {key}:\n{df.head()}")

            # Assign the processed DataFrame to the respective key
            self.dict_data[key]["company"] = df



    def getTime(self, data):
        '''
        Extract time columns for comparison \n
        Input: data: DataFrame containing time columns \n
        Output: Filtered list of years (e.g., only "2023") \n
        '''
        # Get all columns except the first one (assuming it's "Feature")
        all_years = data.columns[1:]

        # Filter to only include "2023"
        filtered_years = [year for year in all_years if year == "2023"]
        print(f"Filtered years for comparison: {filtered_years}")

        return filtered_years


    def get_field(self, key_1, key_2):
        '''
        Compare data from two sources \n
        Input: key_1: Source 1 key \n
            key_2: Source 2 key \n
        Output: DataFrame with comparison results \n
        '''
        df = pd.merge(self.dict_data[key_1]["company"], self.dict_data[key_2]["company"], on=["Feature"], how="inner")
        print(f"Merged data for comparison ({key_1} vs {key_2}):\n{df.head()}")

        # Extract and filter years for comparison
        list_year = self.getTime(self.dict_data["CF"]["company"])
        print(f"Years extracted for comparison: {list_year}")

        s_a, s_b = self.dict_data[key_1]["money"], self.dict_data[key_2]["money"]

        for year in list_year:
            print(f"Comparing data for year: {year}")
            df["Compare"] = df.apply(lambda row: self.compare_2_block(
                row[f"{year}_x"], row[f"{year}_y"], s_a, s_b, row["Feature"]
            ), axis=1)

        print(f"Final comparison data:\n{df.head()}")
        return df

