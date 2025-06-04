import sys
sys.path.append('/app/')
from Flow import Folder
from VAR_GLOBAL_CONFIG import *

FC = Folder.FolderCrawl()
FU = Folder.FolderUpdate(END_DAY_UPDATE)
FR = Folder.FolderData("","")

DAY_GET = FC.GetDateUpdate(END_DAY_UPDATE)
DAY_RUN = END_DAY_UPDATE

# PATH_FI = FC.joinPath(FC.PATH_MAIN, DAY_GET)
PATH_FT = FU.joinPath(FU.PATH_MAIN,DAY_RUN)
PATH_COMPARE = FU.joinPath(FU.PATH_MAIN,DAY_RUN,"Compare")
LINK_QUATER = "Quarter"
LINK_YEAR = "Year"
BALANCE = "BalanceSheet"
INCOME = "IncomeStatement"
PRICE = "Close"
VOLUME_ADDITIONAL = "Volume/VolumeAdditionailEvents"
VOLUME_TREASURY_SHARE = "Volume/TreasuryShares"
DIVIDEND = "Dividend"
VOLUME="Volume"
VOLUME_NOW = "VolumeNow"
dict_path_cf = {
                "Feature": FR.PATH_MAIN,
                "F0":{"Balance_Year": FC.joinPath(PATH_FT,"Financial/CafeF","F0",LINK_YEAR,BALANCE),
                    "Income_Year": FC.joinPath(PATH_FT,"Financial/CafeF","F0",LINK_YEAR,INCOME),
                    "Balance_Quarter": FC.joinPath(PATH_FT,"Financial/CafeF","F0",LINK_QUATER,BALANCE),
                    "Income_Quarter": FC.joinPath(PATH_FT,"Financial/CafeF","F0",LINK_QUATER,INCOME),
                    'Price': FC.joinPath(PATH_FT,PRICE,"CafeF"),
                    'Dividend': FC.joinPath(PATH_FT,DIVIDEND,"CafeF","F0"),
                    'VolumeNow':FC.joinPath(PATH_FT,VOLUME,"CafeF","F0","VolumeNow"),
                    },
                "F1":{
                    'Dividend': FC.joinPath(PATH_FT,DIVIDEND,"CafeF","F1"),
                    "Year": FC.joinPath(PATH_FT,"Financial/CafeF","F1",LINK_YEAR),
                    "Quarter": FC.joinPath(PATH_FT,"Financial/CafeF","F1",LINK_QUATER),
                    },
                "F2":{
                    "Year": FC.joinPath(PATH_FT,"Financial/CafeF","F2",LINK_YEAR),
                    "Quarter": FC.joinPath(PATH_FT,"Financial/CafeF","F2",LINK_QUATER),
                    },
                "F3":{
                    "Year": FC.joinPath(PATH_FT,"Financial/CafeF","F3",LINK_YEAR),
                    "Quarter": FC.joinPath(PATH_FT,"Financial/CafeF","F3",LINK_QUATER),
                    },
                }

dict_path_vs = { "Feature": FR.PATH_MAIN,
                "F0":{"Balance_Year": FC.joinPath(PATH_FT,"Financial/VietStock","F0",LINK_YEAR,BALANCE),
                    "Income_Year": FC.joinPath(PATH_FT,"Financial/VietStock","F0",LINK_YEAR,INCOME),
                    "Balance_Quarter": FC.joinPath(PATH_FT,"Financial/VietStock","F0",LINK_QUATER,BALANCE),
                    "Income_Quarter": FC.joinPath(PATH_FT,"Financial/VietStock","F0",LINK_QUATER,INCOME),
                    'Price': FC.joinPath(PATH_FT,PRICE,"VietStock"),
                    'BonusShare': FC.joinPath(PATH_FT,DIVIDEND,"VietStock","F0",'BonusShare'),
                    'CashDividend': FC.joinPath(PATH_FT,DIVIDEND,"VietStock","F0",'CashDividend'),
                    'StockDividend': FC.joinPath(PATH_FT,DIVIDEND,"VietStock","F0",'StockDividend'),
                    'VolumeNow':FC.joinPath(PATH_FT,VOLUME,"VietStock","F0","VolumeNow"),
                    },
                "F1":{
                    'Dividend': FC.joinPath(PATH_FT,DIVIDEND,"VietStock","F1"),
                    "Year": FC.joinPath(PATH_FT,"Financial/VietStock","F1",LINK_YEAR),
                    "Quarter": FC.joinPath(PATH_FT,"Financial/VietStock","F1",LINK_QUATER),
                    },
                "F2":{
                    "Year": FC.joinPath(PATH_FT,"Financial/VietStock","F2",LINK_YEAR),
                    "Quarter": FC.joinPath(PATH_FT,"Financial/VietStock","F2",LINK_QUATER),
                    },
                "F3":{
                    "Year": FC.joinPath(PATH_FT,"Financial/VietStock","F3",LINK_YEAR),
                    "Quarter": FC.joinPath(PATH_FT,"Financial/VietStock","F3",LINK_QUATER),
                    },
             }