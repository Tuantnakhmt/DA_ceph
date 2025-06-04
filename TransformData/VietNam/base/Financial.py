import datetime
import json
import math
import pandas as pd
import re
import os
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'
import sys
sys.path.append('/app/')
from VAR_GLOBAL_CONFIG import *

class TransForm():
    '''
    Chuyển đổi dữ liệu từ dạng F0 sang F3 \n'''
    def __init__(self,dict_path_) -> None:
        self.path_object = dict_path_
        self.time = []
        pass
    def replace_NaN_0(self,df):
        '''
        Thay thế NaN bằng 0 \n
        Input: df: DataFrame \n
        Output: DataFrame \n'''
        df = df.dropna(axis=1, how='all')
        df = df.fillna(0)
        for column in self.time:
            if not(column in df.columns):
                df[column]=[np.nan for i in df["Feature"]]
        return df
        
    def getTime(self,type_time):
        '''
        Lấy thời gian \n
        Input: type_time: loại thời gian \n
        Output: thời gian \n'''
        if type_time == "Year":
            return YEAR_KEY
        elif type_time == "Quarter":
            return QUARTER_KEY

class CafeF(TransForm):
    '''
    Chuyển đổi dữ liệu từ dạng F0 sang F3 nguồn CafeF\n

    '''
    def __init__(self,dict_path_cf) -> None:
        '''
        Khởi tạo \n
        Input:\n 
        self.data_field: dữ liệu trường cần lấy \n
        self.data_field_default_year: dữ liệu trường mặc định năm \n
        self.data_field_default_quarter: dữ liệu trường mặc định quý \n
        dict_path_cf: đường dẫn \n
        Output: None \n
        '''
        super().__init__(dict_path_cf)
        file = FILE_FEATURE
        df = pd.read_excel(f'{dict_path_cf["Feature"]}/{file}',sheet_name="CafeF")
        df = df.rename(columns={"VIS_Raw_F1":"field"})
        self.data_field = df
        df = pd.read_excel(f'{dict_path_cf["Feature"]}/{file}',sheet_name="Total")
        self.data_field_default_year = df
        df = pd.read_excel(f'{dict_path_cf["Feature"]}/{file}',sheet_name="Quarter")
        self.data_field_default_quarter = df
    
    def CheckData(self,symbol,type_time,time_detail):
        '''
        Kiểm tra dữ liệu đã được crawl chưa \n
        Input: symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        time_detail: thời gian chi tiết \n
        Output: True: đã được crawl, False: chưa được crawl \n
        '''
        for key in self.path_object["F0"].keys():
            if key.find(type_time) != -1:
                    with open(f'{self.path_object["F0"][key]}/{symbol}.json',encoding='utf8') as j:
                            data1 = json.loads(j.read())
                    if pd.isna(data1[time_detail][1][time_detail]):
                        return False
                    else:
                        return True
        return False


    def Financial_F0_to_F1(self,symbol,type_time):
            '''
            Chuyển đổi dữ liệu từ dạng F0 sang F1 \n
            Input: \n
            symbol: mã cổ phiếu \n
            type_time: loại thời gian \n
            Output: DataFrame \n'''

            data = {}
            for key in self.path_object["F0"].keys():
                if key.find(type_time) != -1:
                    try:
                        with open(f'{self.path_object["F0"][key]}/{symbol}.json', encoding='utf8') as j:
                            data1 = json.loads(j.read())
                            # print(f"Loaded data1 for {symbol}: {data1}")  # Debugging line
                        
                        # Initialize an empty list to hold the processed rows
                        processed_data = []

                        # Iterate through each year (2024, 2023, etc.)
                        for year in data1:
                            for record in data1[year]:
                                field = record["field"]
                                value = record.get(year, np.nan)  # Get the value for the given year or NaN if not present
                                processed_data.append([field, value, year])

                        # Convert the processed data into a DataFrame
                        df = pd.DataFrame(processed_data, columns=["field", "value", "year"])
                        # print(f"Processed DataFrame for {symbol}: \n{df}")  # Debugging line

                        # Pivot the DataFrame to get fields as rows and years as columns
                        df_pivoted = df.pivot_table(index="field", columns="year", values="value", aggfunc="first")
                        # print(f"Pivoted DataFrame for {symbol}: \n{df_pivoted}")  # Debugging line

                        # Merge the processed data into the main dictionary
                        for key, value in data1.items():
                            try:
                                if key in data:
                                    data[key] += data1[key]
                                else:
                                    data[key] = data1[key]
                                # print(f'1- data[key] for {key}: {data[key]}')  # Debugging line
                            except Exception as e:
                                print(f"Error merging data for {key}: {e}")  # Debugging line

                    except Exception as e:
                        print(f"Error processing file for {symbol}: {e}")  # Debugging line

            # print(f'1- Full data: {data}')

            temp = pd.DataFrame({"field": []})
            for key in list(data.keys()):
                try:
                    df = pd.DataFrame.from_records(data[key])
                    dict_ = {}
                    for i in df.index:
                        df["field"][i] = ''.join([i for i in df["field"][i] if not i.isdigit()])
                        dict_[df["field"][i]] = 0
                
                    for i in df.index:
                        dict_[df["field"][i]]+=1
                        df["field"][i] = df["field"][i]+"__"+str(dict_[df["field"][i]])
                        col_key = []

                    for i in df:
                        if i in temp.columns:
                            col_key.append(i)
                    temp = pd.merge(temp, df, on=col_key, how="outer")
                except:
                    pass

            arr = []
            for col in temp.columns:
                try:
                    # Remove square brackets from the year in the column names
                    match = re.findall(r'\[(\d{4})\]', col)
                    if match:
                        arr.append(match[0])  # Only append the year without brackets
                    else:
                        arr.append(col)  # Keep other columns unchanged
                except Exception as e:
                    print(f"Error processing column '{col}': {e}")  # Debugging line
                    arr.append(col)

            temp.columns = arr
            temp.to_csv(f'{self.path_object["F1"][type_time]}/{symbol}.csv',index=False)
            return temp

    def Financial_F1_to_F2(self,symbol,type_time):
                '''
                Chuyển đổi dữ liệu từ dạng F1 sang F2 \n
                Input: \n
                symbol: mã cổ phiếu \n
                type_time: loại thời gian \n
                Output: DataFrame \n
                '''
                link ="{}/{}.csv".format(self.path_object["F1"][type_time],symbol)
                data = pd.read_csv(link)
                print(link)
                print(self.data_field.columns)
                print(data.columns)

                temp = pd.merge(self.data_field, data, on="field", how="outer")
                print(f"Merged data columns: {temp.columns}")
                
                # Remove suffixes like '__1', '__2', etc. from the 'field' column
                temp["field"] = temp["field"].str.replace(r"__\d+$", "", regex=True)

                # Drop the 'field' column and convert others to float
                for column in temp.columns[1:]:
                    try:
                        temp[column] = temp[column].astype(float)
                    except ValueError:
                        print(f"Could not convert {column} to float. Filling with NaN.")
                        temp[column] = pd.to_numeric(temp[column], errors='coerce')
                
                # Group by 'field' and take the max value
                if "field" in temp.columns:
                    temp = temp.groupby("field").max().reset_index()
                else:
                    print("'field' column not found in the merged data")
                    return None
                
                # Clean the column names
                temp.columns = [col.replace("]", "").replace("[", "") for col in temp.columns]
                
                # Debugging: Check the final structure
                print(f"Final columns after cleaning: {temp.columns}")
                
                # Save to F2
                output_path = f'{self.path_object["F2"][type_time]}/{symbol}.csv'
                print(f"Saving to: {output_path}")
                
                temp.to_csv(output_path, index=False)
                
                
    def Financial_F2_to_F3(self,symbol,type_time):
                '''
                Chuyển đổi dữ liệu từ dạng F2 sang F3 \n
                Input: \n
                symbol: mã cổ phiếu \n
                type_time: loại thời gian \n
                Output: DataFrame \n'''
                # return temp
                if type_time == "Year":
                    data_field = self.data_field_default_year
                elif type_time == "Quarter":
                    data_field = self.data_field_default_quarter
                else:
                    print(f"Invalid type_time: {type_time}. Must be 'Year' or 'Quarter'.")
                    return None


                # Ensure `Feature` column exists in data_field
                if "Feature" not in data_field.columns:
                    print("Default field is missing the 'Feature' column. Renaming...")
                    if "column" in data_field.columns:
                        data_field.rename(columns={"column": "Feature"}, inplace=True)
                    else:
                        print("No suitable column to rename to 'Feature'. Exiting.")
                        return None

                # Debug: Check renamed data_field
                #print(f"Renamed default field data:\n{data_field.head()}")

                # Load data from F2
                link = f"{self.path_object['F2'][type_time]}/{symbol}.csv"
                #print(f"Reading F2 data from: {link}")

                try:
                    data = pd.read_csv(link)
                except FileNotFoundError:
                    print(f"File not found: {link}")
                    return None

                # Debug: Check F2 data columns
                #print(f"F2 Data columns: {data.columns}")

                # Replace NaN values in data with 0
                data = self.replace_NaN_0(data)
                #print("Replaced NaN with 0.")

                # Standardize column names for merging
                if "field" in data.columns:
                    data.rename(columns={"field": "Feature"}, inplace=True)
                elif "Feature" not in data.columns:
                    print("Merge failed: No 'field' or 'Feature' column found in F2 Data.")
                    return None

                # Debug: Check renamed F2 data
                #print(f"Renamed F2 data:\n{data.head()}")

                # Perform the merge
                if "Feature" in data.columns and "Feature" in data_field.columns:
                    temp = pd.merge(data_field, data, on="Feature", how="inner")
                else:
                    #print("Merge failed: No common column ('Feature') found.")
                    return None

                # Debug: Check merged data
                print(f"Merged data:\n{temp.head()}")

                try:
                    # Debug: Get the time columns and print them
                    time_columns = self.getTime(type_time)
                    print(f"Time columns from getTime: {time_columns}")

                    # Debug: Check the columns of temp
                    print(f"Columns in temp before filtering: {temp.columns}")

                    # Ensure the required columns are present
                    missing_columns = [col for col in time_columns if col not in temp.columns]
                    if missing_columns:
                        print(f"Missing time columns in temp: {missing_columns}")
                        print("Cannot filter columns. Exiting...")
                        return None

                    # Filter temp to include only 'Feature' and valid time columns
                    temp = temp[["Feature"] + time_columns]
                except KeyError as e:
                    print(f"KeyError: {e}. Possible issue with getTime or column selection.")
                    return None

                try:
                    for col in time_columns:
                        if temp[col].dtype in ['int64', 'float64']:  # Ensure the column is numeric
                            temp[col] = temp[col] / 1000
                    print(f"Values in time columns divided by 1000.")
                except Exception as e:
                    print(f"Error during division: {e}. Exiting...")
                    return None

                # Save the resulting F3 data
                output_path = f"{self.path_object['F3'][type_time]}/{symbol}.csv"
                print(f"Saving F3 data to: {output_path}")

                try:
                    temp.to_csv(output_path, index=False)
                    print("F3 data saved successfully.")
                except Exception as e:
                    print(f"Error saving F3 data: {e}")
                    return None

                # temp.to_csv(output_path, index=False)

                return temp
    
    def run(self,symbol,type_time):
        '''
        Chạy chuyển đổi dữ liệu \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: True: thành công, False: thất bại \n
        '''
        try:
            self.Financial_F0_to_F1(symbol,type_time)
            self.Financial_F1_to_F2(symbol,type_time)
            self.Financial_F2_to_F3(symbol,type_time)
        except:
            print(symbol,type_time,"CF")
            return False
        return True


class VietStock(TransForm):
    '''
    Chuyển đổi dữ liệu từ dạng F0 sang F3 nguồn VietStock\n
    '''
    def __init__(self,dict_path_vs) -> None:
        '''
        Khởi tạo \n
        Input:\n
        self.data_field: dữ liệu trường cần lấy \n
        self.data_field_default_year: dữ liệu trường mặc định năm \n
        self.data_field_default_quarter: dữ liệu trường mặc định quý \n
        dict_path_vs: đường dẫn \n
        Output: None \n
        '''
        super().__init__(dict_path_vs)
        df = pd.read_excel(f'{dict_path_vs["Feature"]}/{FILE_FEATURE}',sheet_name="VietStock")
        df = df.rename(columns={"VIS_Raw_F1":"field"})
        self.data_field = df
        df = pd.read_excel(f'{dict_path_vs["Feature"]}/{FILE_FEATURE}',sheet_name="Total")
        self.data_field_default_year = df
        df = pd.read_excel(f'{dict_path_vs["Feature"]}/{FILE_FEATURE}',sheet_name="Quarter")
        self.data_field_default_quarter = df

    def CheckData(self,symbol,type_time,time_detail):
        '''
        Kiểm tra dữ liệu đã được crawl chưa \n
        Input: symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        time_detail: thời gian chi tiết \n
        Output: True: đã được crawl, False: chưa được crawl \n
        '''
        for key in self.path_object["F0"].keys():
            if key.find(type_time) != -1:
                path_in = self.path_object["F0"][key]
                if os.path.exists(f'{path_in}/{symbol}.csv'):
                    df = pd.read_csv(f'{path_in}/{symbol}.csv')
                    try:
                        df[time_detail]
                        return True
                    except:
                        return False
                    
                else:
                    return False
    def change_data_BS(self,df_finan):
        '''
        Chuyển đổi dữ liệu báo cáo tài chính cân đối \n
        Input: df_finan: dữ liệu báo cáo tài chính cân đối \n
        Output: dữ liệu báo cáo tài chính cân đối \n
        '''
        first_col = df_finan.columns[0]
        feature_change  = '- Nguyên giá__' + df_finan[first_col].loc[df_finan[df_finan[first_col]=='- Nguyên giá'].index-1]
        feature_change.index = feature_change.index+1
        df_finan[first_col].iloc[df_finan[df_finan[first_col]=='- Nguyên giá'].index] = feature_change

        feature_change  = '- Giá trị hao mòn lũy kế__' + df_finan[first_col].loc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế '].index-2]
        feature_change.index = feature_change.index+2
        df_finan[first_col].iloc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế '].index] = feature_change

        feature_change  = '- Giá trị hao mòn lũy kế__' + df_finan[first_col].loc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế'].index-2]
        feature_change.index = feature_change.index+2
        df_finan[first_col].iloc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế'].index] = feature_change
        
        return df_finan
    
    def Financial_F0_to_F1(self,symbol,type_time):
        '''
        Chuyển đổi dữ liệu từ dạng F0 sang F1 \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: DataFrame \n

        '''
        data = pd.DataFrame({})
        for key in self.path_object["F0"].keys():
            if key.find(type_time) != -1:
                path_in = self.path_object["F0"][key]
                if os.path.exists(f'{path_in}/{symbol}.csv'):
                    df = pd.read_csv(f'{path_in}/{symbol}.csv')
                    df = self.change_data_BS(df)
                    data = pd.concat([data,df],ignore_index=True)
        path_out = self.path_object["F1"][type_time]
        data.to_csv(f'{path_out}/{symbol}.csv', index = False)
        # print(path_out)

    def Financial_F1_to_F2(self,symbol,type_time):
        '''
        Chuyển đổi dữ liệu từ dạng F1 sang F2 \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: DataFrame \n
        '''
        df = pd.read_csv(f'{self.path_object["F1"][type_time]}/{symbol}.csv')
    
        # Check if dataframe is empty
        if len(df.index) == 0:
            print(f"Warning: The file {symbol}.csv is empty. Skipping F2 processing.")
            return df
        
        # Skip first 6 rows, reset index
        df = df[6:].reset_index(drop=True)
        
        # Rename the first column to 'field'
        first_col = df.columns[0]
        df = df.rename(columns={first_col: 'field'})
        
        # Merge the data with self.data_field
        df_concat = pd.merge(self.data_field, df, how='left', on=['field'])
        
        # Check if merge was successful (no missing 'field' values)
        if df_concat['field'].isnull().any():
            print(f"Warning: Missing 'field' values after merge for symbol {symbol}.")
            return False  # Return False if merge didn't match
        #print(df_concat)
        # Clean up dataframe and rename columns
        df = df_concat.drop(columns=['Unnamed: 1', 'Unnamed: 2','2022'])
        df = df.rename(columns={"field": "Feature"})
        
        # Clean up column names (remove 'Q')
        df.columns = [col.replace("Q", "") for col in df.columns]
        
        # Save the resulting F2 data
        path_out = self.path_object["F2"][type_time]
        output_file = f'{path_out}/{symbol}.csv'
        
        # Check if the dataframe is empty before saving
        if df.empty:
            print(f"Warning: After processing, the resulting dataframe for {symbol} is empty.")
            return df
        
        df.to_csv(output_file, index=False)
        print(f"Successfully saved F2 data for {symbol} to {output_file}.")
        return df

    def Financial_F2_to_F3(self,symbol,type_time):
            '''
            Chuyển đổi dữ liệu từ dạng F2 sang F3 \n
            Input: \n
            symbol: mã cổ phiếu \n
            type_time: loại thời gian \n
            Output: DataFrame \n
            '''
            # Determine the correct data_field based on the type_time
            print(f"Starting F2 to F3 transformation for {symbol} ({type_time})...")
            
            if type_time == "Year":
                data_field = self.data_field_default_year
                print("Using Year data field")
            elif type_time == "Quarter":
                data_field = self.data_field_default_quarter
                print("Using Quarter data field")
            else:
                print(f"Error: Unrecognized type_time: {type_time}. Skipping F3 processing.")
                return None
            
            # Read F2 data
            link = "{}/{}.csv".format(self.path_object["F2"][type_time], symbol)
            print(f"Attempting to read F2 data from: {link}")
            
            try:
                data = pd.read_csv(link)
                print(f"Successfully read F2 data for {symbol}.")
            except FileNotFoundError:
                print(f"Error: File {symbol}.csv not found in F2. Skipping.")
                return None

            # Get time columns from F2 data
            self.time = data.columns[1:]
            print(f"Identified time columns: {self.time}")
            
            # Replace NaN values with 0 (if necessary)
            data = self.replace_NaN_0(data)
            print(f"NaN values replaced with 0.")
            
            # Merge the F2 data with the data_field
            print(f"Merging F2 data with data_field on 'Feature' column...")
            data_field.rename(columns={"column": "Feature"}, inplace=True)
            temp = pd.merge(data_field, data, on="Feature", how="inner")
            
            # Ensure the merge resulted in data; otherwise, skip the saving
            if temp.empty:
                print(f"Warning: Merge failed for {symbol}. No matching 'Feature' found.")
                return None
            
            time_columns = self.getTime(type_time)
            print(f"Attempting to select time columns: {time_columns}")
            
            # Ensure the time columns exist in the merged dataframe
            if all(col in temp.columns for col in time_columns):
                temp = temp[[data.columns[0]] + time_columns]
            else:
                print(f"Warning: Time columns {time_columns} not found in the merged data. Adding NaN for missing time columns.")
                for col in time_columns:
                    if col not in temp.columns:
                        temp[col] = np.NaN
                temp = temp[[data.columns[0]] + time_columns]
            
            # Check if temp dataframe is empty or not before saving
            if temp.empty:
                print(f"Error: After processing, the resulting dataframe for {symbol} is empty. Skipping F3 save.")
                return None
            
            # Save the resulting F3 data
            output_path = f"{self.path_object['F3'][type_time]}/{symbol}.csv"
            print(f"Saving F3 data for {symbol} to {output_path}")
            temp.to_csv(output_path, index=False)
            print(f"Successfully saved F3 data for {symbol} to {output_path}.")
            
            return temp

    def run(self,symbol,type_time):
        '''
        Chạy chuyển đổi dữ liệu \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: True: thành công, False: thất bại \n
        '''
        try:
            self.Financial_F0_to_F1(symbol,type_time)
            self.Financial_F1_to_F2(symbol,type_time)
            self.Financial_F2_to_F3(symbol,type_time)
        except:
            print(symbol,type_time,"VS")
            return False
        return True
