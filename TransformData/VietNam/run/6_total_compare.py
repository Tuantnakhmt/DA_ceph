import code
import pandas as pd
import sys

sys.path.append(r'D:\DATN')
sys.path.append(r'D:\DATN\TransformData\VietNam')

from base.PATH_UPDATE import *
from base.Setup import *


dict_compare = {
    # "Financial_Quarter": pd.DataFrame(),
    "Financial_Year": pd.DataFrame(),
    # "Dividend": pd.DataFrame(),
    # "Volume": pd.DataFrame()
}

def GetResultCompare(symbol,field):
    path = field.replace("_","/")
    df_current = pd.read_csv(f"{PATH_COMPARE}/{path}/{symbol}.csv")
    df_current["Symbol"] = [symbol for i in df_current.index]
    dict_compare[field] = pd.concat([dict_compare[field],df_current],ignore_index=True)
    return df_current

def GetError(data,CodeError):
    data["Check"] = data["Compare"].apply(lambda row: row in CodeError)
    return data[data["Check"]==True]

CodeError = [2,0,"2","0","N"]
for key in dict_compare.keys():
    for symbol in SYMBOL:
        try:
            GetResultCompare(symbol,key)
        except:
            continue
    dict_compare[key].to_excel(f"{PATH_COMPARE}/{key}.xlsx",index=False)
    GetError(dict_compare[key],CodeError).to_excel(f"{PATH_COMPARE}/Error/{key}{CodeError}.xlsx",index=False)