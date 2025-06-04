import json
import time
import pandas as pd
import shutil
import sys
sys.path.append(r'D:\test_storage')
sys.path.append(r'D:\test_storage\TransformData\VietNam')
from Flow.ulis import *
from base.Setup import *
from base.Dividend import *
pd.options.mode.chained_assignment = None
T = TieuChuan()

def read_file(path,file_type,field,source):
    '''
    kiểm tra file có tồn tại và thỏa man tiêu chuẩn không \n'''
    if file_type == ".csv":
        try:
            data = pd.read_csv(path)
        except:
            return False
    elif file_type == ".json":
        try:
            f = open(path, encoding="utf8")
            data = json.load(f)
        except:
            return False
    return T.CheckDataFinancial(field,data,source=source)  


def File(list_symbol,F_RANGE,source,type_time,field,file_type=".csv",type_data = "Financial"):
    CURRENT = 0
    for symbol in list_symbol:
        CURRENT+=1
        progress_bar(CURRENT,TOTAL,text=f"Lấy {type_data}:{field} --- {source}")
        for index in range(len(F_RANGE)-1,-1,-1):
            date = F_RANGE[index]
            path = FC.joinPath(FC.PATH_MAIN,date,type_data,source,type_time,field,f"{symbol}{file_type}")
            if read_file(path,file_type,field,source) == True:
                path_from = path
                path_to = FC.joinPath(FU.PATH_MAIN,F_END,type_data,source,"F0",type_time,field)
                print(path_to)
                shutil.copy2(path_from, path_to)
                break

def FileVersionFinancialNew(list_symbol,F_RANGE,source,type_time,file_type=".csv",type_data = "Financial"):
    '''Quan tâm đến thời gian, tính thời điểm'''
    temp_stack = 0
    for i in list_symbol[::50]:
        for index in range(len(F_RANGE)-1,-1,-1):
            date = F_RANGE[index]
            path = FC.joinPath(FC.PATH_MAIN,date,type_data,source,type_time,f"{temp_stack}{file_type}")
            if read_file(path,file_type,"BalanceSheet",source) == True:
                path_from = path
                path_to = FC.joinPath(FU.PATH_MAIN,F_END,type_data,source,"Temp")
                path_to_to = FC.joinPath(FU.PATH_MAIN,F_END,type_data,source,"F0",type_time)
                Split_File_CSV_VS(list_symbol[temp_stack:temp_stack+50],path_from,path_to_to,type_data)
                shutil.copy2(path_from, path_to)
                break
        temp_stack += 50
        progress_bar(temp_stack,TOTAL,text=f"Lấy {type_data} --- {source}")

def FileVersionDividendNew(list_symbol,F_RANGE,source,file_type=".csv",type_data = "Dividend"):
    '''Không quan tâm đến thời gian, tính thời điểm'''

    for index in range(len(F_RANGE)-1,-1,-1):
        date = F_RANGE[index]
        path = FC.joinPath(FC.PATH_MAIN,date,type_data,source,f"{type_data}{file_type}")
        if read_file(path,file_type,"",source) == True:
            print(path)
            path_from = path
            path_to = FC.joinPath(FU.PATH_MAIN,F_END,type_data,source,"Temp")
            path_to_to = FC.joinPath(FU.PATH_MAIN,F_END,type_data,source,"F0")
            Split_File_CSV_VS(list_symbol,path_from,path_to_to,type_data)
            shutil.copy2(path_from, path_to)
            break
    progress_bar(TOTAL,TOTAL,text=f"Lấy {type_data} --- {source}")




def delete_last_number(string):
    for i in range(len(string)-1,-1,-1):    
        if string[i].isdigit() == True:
            string = string[:i]
        else:
            break
    return string


def Split_File_CSV_VS(list_symbol,path_from,path_to,type_data="Dividend"):
    file_csv = pd.read_csv(path_from)

    if type_data == "Dividend":
        for symbol in list_symbol: 
            df_temp = file_csv[file_csv["Mã CK"] == symbol]

            for part in FU.DividendPartObject:
                if part == "CashDividend":
                    df = df_temp[df_temp["Loại Sự kiện"] == "Trả cổ tức bằng tiền mặt"]
                elif part == "BonusShare":
                    df = df_temp[df_temp["Loại Sự kiện"] == "Thưởng cổ phiếu"]
                elif part == "StockDividend":
                    df = df_temp[df_temp["Loại Sự kiện"] == "Trả cổ tức bằng cổ phiếu"]
                # df = df[["STT","Mã CK","Sàn","Ngày GDKHQ▼","Ngày ĐKCC","Tỷ lệ","Khối lượng dự kiến phát hành","Sự kiện"]]
                df.to_csv(path_to +"/"+ part + f"/{symbol}.csv",index=False)

    elif type_data == "Financial":
        file_csv["Mã CK"] = file_csv["Mã CK"].apply(lambda x: delete_last_number(x))
        for symbol in list_symbol:
            try:
                df_temp = file_csv[["Mã CK",f"{symbol}"]]
                df_temp.columns = df_temp.iloc[0]
                df_temp.columns = [col.replace("Quý","Q").replace(" ","").replace("Kỳ/năm",symbol) for col in df_temp.columns]
                df_temp["Unnamed: 1"] = [None for i in range(len(df_temp))]
                df_temp["Unnamed: 2"] = [None for i in range(len(df_temp))]
                df_temp["TempTime"] = [None for i in range(len(df_temp))]
                df_temp = df_temp[[f"{symbol}","Unnamed: 1","Unnamed: 2","TempTime",df_temp.columns[1]]]
                col_temp = [df_temp.columns[-1]]
            except KeyError:
                col_temp = [f"Q{QUARTER_KEY}"]
                df_temp = file_csv[["Mã CK"]].rename(columns={"Mã CK":f"{symbol}"})
                df_temp["Unnamed: 1"] = [None for i in range(len(df_temp))]
                df_temp["Unnamed: 2"] = [None for i in range(len(df_temp))]
                df_temp["TempTime"] = [None for i in range(len(df_temp))]
                df_temp[col_temp[-1]] = [None for i in range(len(df_temp))]

            stack,idx  = 0,0
            for part in FU.FinancialPartObject:
                stack = idx
                if part == "BalanceSheet":
                    idx = df_temp[df_temp[symbol] == "KẾT QUẢ KINH DOANHMS"].index[0]
                    df_temp[stack:idx].to_csv(path_to +"/"+ part + f"/{symbol}.csv",index=False)
                elif part == "IncomeStatement":
                    idx = df_temp[df_temp[symbol] == "LƯU CHUYỂN TIỀN TỆ TRỰC TIẾPMS"].index[0]
                    df_temp[stack:idx].to_csv(path_to +"/"+ part + f"/{symbol}.csv",index=False)
                elif part == "CashFlowDirect":
                    pass
                elif part == "CashFlowInDirect":
                    pass
    return         

        
# File(SYMBOL,F_RANGE,source="CafeF",type_time="",field="",type_data="Dividend")
# FileVersionDividendNew(SYMBOL,F_RANGE,"VietStock",type_data="Dividend")

# File(SYMBOL,F_RANGE,"VietStock","Quarter","BalanceSheet")

File(SYMBOL,F_RANGE,"VietStock","Year","BalanceSheet")

File(SYMBOL,F_RANGE,"VietStock","Year","IncomeStatement")
# File(SYMBOL,F_RANGE,"VietStock","Quarter","BalanceSheet")
# File(SYMBOL,F_RANGE,"VietStock","Quarter","IncomeStatement")

# FileVersionFinancialNew(SYMBOL,F_RANGE,"VietStock","Quarter")

File(SYMBOL,F_RANGE,"CafeF","Year","BalanceSheet",".json")
File(SYMBOL,F_RANGE,"CafeF","Year","IncomeStatement",".json")

# File(SYMBOL,F_RANGE,"CafeF","Quarter","BalanceSheet",".json")
# File(SYMBOL,F_RANGE,"CafeF","Quarter","IncomeStatement",".json")

# File(SYMBOL,F_RANGE,source="CafeF",type_time="",field="",type_data="Close")

# File(SYMBOL,F_RANGE,source="CafeF",type_time="",field="VolumeNow",type_data="Volume")
# File(SYMBOL,F_RANGE,source="VietStock",type_time="",field="VolumeNow",type_data="Volume")

# File(SYMBOL,F_RANGE,source="VietStock",type_time="",field="CashDividend",type_data="Dividend")
# File(SYMBOL,F_RANGE,source="VietStock",type_time="",field="BonusShare",type_data="Dividend")
# File(SYMBOL,F_RANGE,source="VietStock",type_time="",field="StockDividend",type_data="Dividend")