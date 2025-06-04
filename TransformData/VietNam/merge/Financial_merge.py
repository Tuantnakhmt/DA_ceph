import pandas as pd
import sys

sys.path.append('/app/')
sys.path.append('/app/TransformData/VietNam')
from base.PATH_UPDATE import *
from VAR_GLOBAL_CONFIG import *
from base.Financial import CafeF,VietStock
from base.Setup import *
from Flow.ulis import *

print(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}.xlsx")
# YEAR_FINANCAIL_FIX_FILE_new = "Financial_Year_with_FIX"
Data = pd.read_excel(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}.xlsx",sheet_name="Sheet1")
# Data = pd.read_excel(f"G:\My Drive\DataVIS\VietNam\Data Lake\Raw_VIS/2023-05-17/Compare/Financial_Quarter.xlsx",sheet_name="Sheet1")
# print(Data)
# Data1 = pd.read_excel(f"C:/Users/vangd/Downloads/Financial_Year.xlsx",sheet_name="Phiên bản 1 (31032023)")
# # print(len(Data1.index))
# Data3 = pd.read_excel(f"{PATH_COMPARE}/Financial_Year_HOSE.xlsx",sheet_name="Financial_phiên bản 3")
# # print(len(Data3.index))
# Data4 = pd.read_excel(f"C:/Users/vangd/Downloads/Financial_Year (3).xlsx",sheet_name="Financial_ Phiên bản 4")
# # print(len(Data4.index))
# Data5 = pd.read_excel(f"{PATH_COMPARE}/Financial_Year.xlsx",sheet_name="Financial_Phiên bản 5")
# Data6 = pd.read_excel(f"C:/Users/vangd/Downloads/Financial_Year_HOSE_4-2.xlsx",sheet_name="Financial_Phiên bản 6")

# Data7 = pd.read_excel(f"{PATH_COMPARE}/Financial_Year_HOSE_4-3.xlsx",sheet_name="Financial_Phiên bản 7")
# Data8 = pd.read_excel(f"{PATH_COMPARE}/Financial_Year_HOSE_4-4.xlsx",sheet_name="Financial_Phiên bản 8")
# Data = pd.concat([Data1,Data3,Data4,Data5,Data6,Data7,Data8],ignore_index=True)
# print(len(pd.unique(Data["Symbol"])))

# print(Data)
# DataFix = pd.read_excel(f"{PATH_COMPARE}/{QUARTER_FINANCAIL_FIX_FILE_BY_HUMAN}.xlsx",sheet_name="Sheet1")
current = 0
def read_file(path):
    try:
        return pd.read_csv(path)
    except:
        return pd.DataFrame()

def alalyst_code(code):
    try:
        source = None
        code = int(code)
        if code == 1:
            source = "CafeF"
        elif code == 2 or code ==0:
            source = "FileFix"
        else:
            pass
        return source
    except ValueError:
        return

def getDataFixError(x,y,source):
    if source == "CafeF":
        return x
    elif source == "FileFix":
        return y
    return None

# Data_Source = Data
# DATA = pd.DataFrame()
# for com in SYMBOL:
#     current+=1
#     df = Data_Source[Data_Source["Symbol"] == com]
#     df = df[["Feature","FIX"]].reset_index(drop=True).T
#     df = df.rename(columns=df.iloc[0])
#     df = df.drop(df.index[0]).reset_index(drop=True)
#     df["Symbol"] = [com for i in df.index]
#     DATA = pd.concat([DATA,df],ignore_index=True)
#     progress_bar(current,TOTAL,text="Bien doi hang")
# Data_Source = Data
# DATA = pd.DataFrame()

# for com in SYMBOL:
#     current += 1
    
#     # Filter data for the current symbol
#     df = Data_Source[Data_Source["Symbol"] == com]
    
#     # Select only the necessary columns and reset index
#     df = df[["Feature", "FIX"]].reset_index(drop=True).T
    
#     # Rename columns using the first row
#     df = df.rename(columns=df.iloc[0])
    
#     # Drop the first row (now column names) and reset index
#     df = df.drop(df.index[0]).reset_index(drop=True)
    
#     # Add the 'Symbol' column
#     df["Symbol"] = [com for i in df.index]
    
#     # Ensure column uniqueness before concatenating
#     df.columns = pd.Index(df.columns).str.strip()
    
#     # Concatenate the transformed DataFrame
#     DATA = pd.concat([DATA, df], ignore_index=True)
    
#     # Display progress
#     progress_bar(current, TOTAL, text="Bien doi hang")

Data_Source = Data
DATA = pd.DataFrame()

for com in SYMBOL:
    current += 1

    # Filter data for the current symbol
    df = Data_Source[Data_Source["Symbol"] == com]
    
    # Ensure the "Feature" column is unique
    if not df["Feature"].is_unique:
        df["Feature"] = df["Feature"] + "_" + df.groupby("Feature").cumcount().astype(str)
    
    # Select only the necessary columns and reset index
    df = df[["Feature", "FIX"]].reset_index(drop=True).T

    # Rename columns using the first row
    df = df.rename(columns=df.iloc[0])
    
    # Drop the first row (now column names) and reset index
    df = df.drop(df.index[0]).reset_index(drop=True)

    # Add the 'Symbol' column
    df["Symbol"] = [com for i in df.index]

    # Debug: Check for duplicates or mismatched columns
    print(f"Processing {com}: df columns = {df.columns}")

    # Concatenate the transformed DataFrame
    DATA = pd.concat([DATA, df], ignore_index=True)
    
    # Display progress
    progress_bar(current, TOTAL, text="Bien doi hang")

YEAR_KEY = '_'.join(YEAR_KEY) 
# DATA["Time"] = [QUARTER_KEY for i in DATA.index]
DATA["Time"] = [YEAR_KEY for i in DATA.index]
# DATA.to_excel(f"{FU.PATH_MAIN_CURRENT}/FINANCAIL_{QUARTER_KEY.replace('/','_')}.xlsx",index=False)
DATA.to_excel(f"{FU.PATH_MAIN_CURRENT}/FINANCAIL_{YEAR_KEY.replace('/','_')}.xlsx",index=False)

# Data["Source"] = Data["Compare"].apply(lambda row: alalyst_code(row))
# DataFix = DataFix[["Feature","Symbol","Fix_Trang"]]
# Data_Source = pd.merge(Data,DataFix,how="outer",on=["Feature","Symbol"])
# Data_Source[QUARTER_KEY] = Data_Source.apply(lambda row: getDataFixError(row[f"{QUARTER_KEY}_x"],row["Fix_Trang"],row["Source"]),axis=1)
# Data_Source = Data_Source[["Feature","Symbol",QUARTER_KEY]]
# DATA = pd.DataFrame()
# for com in SYMBOL:
#     current+=1
#     df = Data_Source[Data_Source["Symbol"] == com]
#     df = df[["Feature",QUARTER_KEY]].reset_index(drop=True).T
#     df = df.rename(columns=df.iloc[0])
#     df = df.drop(df.index[0])
#     df["Symbol"] = [com for i in df.index]
#     DATA = pd.concat([DATA,df],ignore_index=True)
#     progress_bar(current,TOTAL,text="Bien doi hang")
# DATA.to_excel(f"{FU.PATH_MAIN_CURRENT}/FINANCAIL_{QUARTER_KEY.replace('/','_')}.xlsx",index=False)