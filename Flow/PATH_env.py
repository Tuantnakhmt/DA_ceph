# import os
# import datetime

# day,month,year=0,0,0
# # day,month,year=1,5,2023
# if day != 0:
#     date = datetime.datetime(year,month,day)
# else:
#     date = datetime.datetime.today()
#     t = date.weekday()
#     if t % 2 == 1:
#         date = date - datetime.timedelta(days=1)


# class PATH_ENV():
#     def __init__(self, Type_, date=date, RealDay=True):
#         """
#         Initialize environment paths.
#         Type_ : str : Type of data ("Ingestion", "Raw_VIS", "WH", etc.)
#         date : str : Specific date in YYYY-MM-DD format
#         RealDay : bool : Whether to use real system date or a provided one
#         """
#         self.base_path = os.getenv("DATA_BASE_PATH", "/app/data")
#         self.setTypeFolder(Type_)

#         if RealDay == True:
#             self.DateCurrent = date
#             self.DayCurrent= date.strftime("%Y-%m-%d")
#         else:
#             self.DayCurrent = date

#     def setTypeFolder(self, Type_):
#         """Set paths based on the type of data."""
#         if Type_ == "Ingestion":
#             self.path_main = os.getenv("INGESTION_PATH", f"{self.base_path}/Ingestion")
#         elif Type_ == "Raw_VIS":
#             self.path_main = os.getenv("RAW_VIS_PATH", f"{self.base_path}/Raw_VIS")
#         elif Type_ == "WH":
#             self.path_main = os.getenv("WH_PATH", f"{self.base_path}/Warehouse")
#         else:
#             raise ValueError(f"Unsupported Type_: {Type_}")

#         self.path_close = os.path.join(self.path_main, self.DayCurrent, "Close")
#         self.path_dividend = os.path.join(self.path_main, self.DayCurrent, "Dividend")
#         self.path_financial = os.path.join(self.path_main, self.DayCurrent, "Financial")
#         self.path_volume = os.path.join(self.path_main, self.DayCurrent, "Volume")

#     @staticmethod
#     def joinPath(*args):
#         """Join multiple parts into a single path."""
#         return os.path.join(*args)
import datetime

# Update the path to the Docker-mounted volume
PATH_Data = "/app/data"

day, month, year = 0, 0, 0
if day != 0:
    date = datetime.datetime(year, month, day)
else:
    date = datetime.datetime.today()
    t = date.weekday()
    if t % 2 == 1:
        date = date - datetime.timedelta(days=1)

class PATH_ENV():
    '''
    Create Path for Data
    '''
    def __init__(self, Type_, date=date, RealDay=True):
        '''
        Type_: Loại data \n
        date: Ngày \n
        RealDay: True: Ngày thực, False: Ngày thực tế \n
        '''
        if RealDay == True:
            self.DateCurrent = date
            self.DayCurrent = date.strftime("%Y-%m-%d")
        else:
            self.DayCurrent = date
        self.setTypeForder(Type_)
        self.CloseObject = ["CafeF", "StockBiz"]
        self.DividendObject = ["CafeF", "VietStock"]
        self.DividendPartObject = ["CashDividend", "BonusShare", "StockDividend"]
        self.FinancialObject = ["CafeF", "VietStock"]
        self.Type_Time = ["Year", "Quarter"]
        self.FinancialPartObject = ["BalanceSheet", "IncomeStatement", "CashFlowDirect", "CashFlowInDirect"]
        self.VolumeObject = ["CafeF", "VietStock", "TVSI"]
        self.VolumePartObject = ["TreasuryShares", "VolumeAdditionailEvents", "VolumeNow"]
        self.Phase = [f"F{i}" for i in range(4)]
        self.Temp = "Temp"

    def joinPath(self, *arg):
        '''
        Nối các đường dẫn thành thư mục \n
        Input: *arg: các đường dẫn \n
        Output: đường dẫn nối'''
        arr = []
        for i in arg:
            if i != "":
                arr.append(i)
        return "/".join(arr)

    def setTypeForder(self, Type):
        '''
        Chọn loại thư mục \n
        Input: Type: Loại thư mục \n
        Output: None'''
        if Type == "Ingestion":
            PATH_Data = "/app/data/Ingestion"
        elif Type == "Raw_VIS":
            PATH_Data = "/app/data/Raw_VIS"
        elif Type == "WH":
            PATH_Data = "/app/data/Warehouse"
            self.PATH_MAIN = PATH_Data
            self.PATH_CLOSE = self.joinPath(self.PATH_MAIN, "Close")
            return 
        else:
            PATH_Data = "/app/data/Data_Rule"
        self.PATH_MAIN = PATH_Data
        self.PATH_MAIN_CURRENT = self.joinPath(self.PATH_MAIN, self.DayCurrent)
        self.PATH_CLOSE = self.joinPath(self.PATH_MAIN, self.DayCurrent, "Close")
        self.PATH_COMPARE = self.joinPath(self.PATH_MAIN, self.DayCurrent, "Compare")
        self.PATH_FINANCIAL = self.joinPath(self.PATH_MAIN, self.DayCurrent, "Financial")
        self.PATH_DIVIDEND = self.joinPath(self.PATH_MAIN, self.DayCurrent, "Dividend")
        self.PATH_VOLUME = self.joinPath(self.PATH_MAIN, self.DayCurrent, "Volume")
        self.REAl_DAY = self.joinPath(self.PATH_MAIN, "RealDay")
        self.REAl_DAY_CLOSE = self.joinPath(self.REAl_DAY, 'Close')
        self.REAl_DAY_IBOARD = self.joinPath(self.REAl_DAY, 'RawIBoardSSI')
