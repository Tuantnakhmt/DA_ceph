import pandas as pd
import numpy as np
import sys
sys.path.append('/app/')
from Flow import Folder
from Flow.ulis import *
# import PATH_UPDATE
import re
import math
from fractions import Fraction

def get_new_row_if_dup(dat):
    """ duplicate in multi rows """
    key_split = '\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0' 
    new_lst = list(dat['New']) 
    time_lst = list(dat['Time'])
    final_new = []
    final_time = []
    for idx in range(len(list(dat['New']))):
        value = list(dat['New'])[idx]
        if key_split in value:
            t= value.split(key_split)
            time = time_lst[idx]
            for item in t :
                final_time.append(time) 
                final_new.append(item)
        else:
            final_time.append(time_lst[idx])
            final_new.append(value)
    dat = pd.DataFrame({'Time': final_time, 'New': final_new})
    return dat

def get_datetime_from_raw(text):
    text = f'{text}'
    if '/' in text:
        text = text.split(': ')[0]
        text = text.replace(' ', '')
        text = text.replace(':', '')
        return text 
    return 'Invalid'

def get_datetime_from_datetime(text):
    text = get_datetime_from_raw(text)
    if text== 'Invalid':
        return text
    split = text.split('/')
    time = "/".join([split[i] for i in range(len(split)-1,-1,-1)])
    return time
def change_type_all(value):
    value = f'{value}'
    if ',' in value:
        value = value.replace(',', '')
    if '%' in value: 
        value =  value.replace('%', '') + '/100'
    if ':' in value :
        value = value.replace(':', '/') 
    return value  

def change_money2percent(text): 
    text = f'{text}'
    if ':' in text:
        text = float(text.split(':')[0]) / float(text.split(':')[1])
        text = f'{text}' + '%'
    return text  

def change_stock2(text): 
    text = f'{text}'
    if '%' in text: 
        text = '100/' + text.replace('%', '')
    
    if ';' in text:
        text = text.replace(';', '')

    return text
def cal_fraction_stock(lst):
    t = 0
    for v in lst:
        v = f'{v}'
        if 'NAN' not in v:
            s1 = v.split('/')
            v11 = s1[0]
            v12 = s1[-1]
            if v12 == '':
                v12 = 100000000
            t += Fraction(f'{v12}') / Fraction(f'{v11}')
    if t == 0:
        return 'NAN'
    else:
        b = t
        re = f'{b.denominator}' + '/' + f'{b.numerator}'
        return re

def cal_fraction_money(lst):
    t = 0
    for v in lst:
        v = f'{v}'
        if 'NAN' not in v:
            s1 = v.split('/')
            v11 = s1[0]
            v12 = s1[-1]
            t += Fraction(f'{v11}') / Fraction(f'{v12}')
    if t == 0:
        return 'NAN'
    else:
        b = t
        re = f'{b.numerator}' + '/' + f'{b.denominator}'
        return re

def loc_nan_nan(dat):
    if dat.shape[0] > 0:
        dat['Status'] = dat.apply(lambda x : loc_nan_nan_support(x['Stock'], x['Money']), axis = 1)
        dat= dat[dat['Status'] == 'Valid']
        return dat[['Time', 'Stock', 'Money']]  
    else:
        dat = pd.DataFrame({'Time': [], 'Stock': [], 'Money': []})
        return dat 
def loc_nan_nan_support(v1, v2):
    if (v1=="NAN") & (v2=="NAN"):
        return 'Invalid' 
    return 'Valid'

class DividendCF():
    def __init__(self,dict_path_={}) -> None:
        self.path_object = dict_path_
        pass
    
    def valid_value(self,text):
        text = f'{text}'
        if 'tỷ lệ ' in text:
            text= text.replace('tỷ lệ ', '')
        if '\n' in text:
            text = text.replace('\n', '')
        if ' ' in text: 
            text = text.replace(' ', '')
        return text

    def identify_cash(self,item):
        value = item.split(', ')
        for ids in range(len(value)-1):
            if 'CỔ TỨC BẰNG TIỀN' in value[ids].upper():
                money = self.valid_value(item.split(', ')[ids+1])
                return money  
        return "NAN"

    def identify_stock(self,item):
        value = item.split(', ')
        for ids in range(len(value)-1):
            if 'CỔ TỨC BẰNG CỔ PHIẾU' in value[ids].upper():
                money = self.valid_value(item.split(', ')[ids+1])
                return money  
            elif 'THƯỞNG BẰNG CỔ PHIẾU' in value[ids].upper():
                money = self.valid_value(item.split(', ')[ids+1])
                return money  
        return "NAN"

    def Dividend_CF(self,symbol):
        # print(self.path_object["F0"]["Dividend"])
        try:
            dat = pd.read_csv(f'{self.path_object["F0"]["Dividend"]}/{symbol}.csv')
        except:
            return pd.DataFrame()
        dat['Time'] = dat['New'].apply(lambda x : get_datetime_from_datetime(x))
        dat = dat[dat['Time'] != 'Invalid']
        dat['Time'] = dat['Time'].apply(lambda x : x.replace("/","-"))
        # dat['Time'] = dat['Time'].apply(lambda x : get_datetime_from_datetime(x))
        dat = get_new_row_if_dup(dat)
        dat['Money'] = dat['New'].apply(lambda x : self.identify_cash(x))
        dat['Stock'] = dat['New'].apply(lambda x : self.identify_stock(x))
        # change format
        dat['Money'] = dat['Money'].apply(lambda x : change_money2percent(x))
        dat['Stock'] = dat['Stock'].apply(lambda x : change_stock2(x))

        dat['Money'] = dat['Money'].apply(lambda x : change_type_all(x))
        dat['Stock'] = dat['Stock'].apply(lambda x : change_type_all(x))
        pivot = pd.pivot_table(data = dat, index = ['Time'],
                           aggfunc = {'Money': lambda x : cal_fraction_money(x),
                                      'Stock': lambda x : cal_fraction_stock(x)})
        pivot = pivot.reset_index()
        pivot = loc_nan_nan(pivot)
        return pivot
    
def get_datetime(text):
    text = f'{text}'
    split = text.split('/')
    time = "/".join([split[i] for i in range(len(split)-1,-1,-1)])
    # valid_format = pd.to_datetime(time)
    return time

def get_cash(path):
    try:
        dat = pd.read_csv(path)
        cash = dat[['Ngày GDKHQ▼','Tỷ lệ']]
        cash.columns = ['Time', 'Money']
        if cash.shape[0] > 0:
            return cash
        else:
            cash = pd.DataFrame({'Time' : [], 'Money': []})
            return cash
    except:
        cash = pd.DataFrame({'Time' : [], 'Money': []})
        return cash

def get_share(path):
    try:
        dat = pd.read_csv(path)
        stock = dat[['Ngày GDKHQ▼', 'Tỷ lệ']]
        stock.columns = ['Time', 'Share']
        if stock.shape[0] >= 1:
            return stock 
        else:
            stock = pd.DataFrame({'Time': [], 'Share': []})
            return stock
    except:
        stock = pd.DataFrame({'Time': [], 'Share': []})
        return stock

def get_bonus(path):
    try:
        dat = pd.read_csv(path)
        stock = dat[['Ngày GDKHQ▼', 'Tỷ lệ']]
        stock.columns = ['Time', 'Bonus']
        if stock.shape[0] > 0:
            return stock
        else:
            stock = pd.DataFrame({'Time': [], 'Bonus': []})
            return stock 
    except:
        stock = pd.DataFrame({'Time': [], 'Bonus': []})
        return stock

def add_keys(x, y, z):
    return f'{x}'+ '/' + f'{y}' + '/' + f'{z}'

def sort_by_time(dat):
    dat['Time'] = dat['Time'].apply(lambda x : get_datetime(x))
    dat['Year'] = dat['Time'].apply(lambda x : x[:4])
    dat['Day'] = dat['Time'].apply(lambda x : x[8:11])
    dat['Month'] = dat['Time'].apply(lambda x : x[5:7])
    if dat.shape[0] > 0:
        dat['Time_valid'] = dat.apply(lambda x : add_keys(x['Day'], x['Month'], x['Year']), axis = 1)
        dat['Year'] = dat['Year'].astype(int)
        dat['Month'] = dat['Month'].astype(int)
        dat['Day'] = dat['Day'].astype(int)
        dat = dat.sort_values(by = ['Year', 'Month', 'Day'])
        return dat 
    else:
        dat = pd.DataFrame({'Time': [], 'Time_valid': [], 'Stock': [], 'Money': []})
        return dat
def merge_bonus_share(v1, v2):
    v1 = f'{v1}'
    v2 = f'{v2}'
    if 'NAN' in v1: 
        return v2
    if 'NAN' in v2: 
        return v1 
    else:
        v = cal_fraction_stock([v1, v2])
        return v

def sort_by_time2(dat):
    dat['Time'] = dat['Time'].apply(lambda x : get_datetime(x))
    dat['Year'] = dat['Time'].apply(lambda x : x[6:11])
    dat['Day'] = dat['Time'].apply(lambda x : x[:2])
    dat['Month'] = dat['Time'].apply(lambda x : x[3:5])
    if dat.shape[0]> 0:
        dat['Time_valid'] = dat.apply(lambda x : add_keys(x['Day'], x['Month'], x['Year']), axis = 1)
        dat['Year'] = dat['Year'].astype(int)
        dat['Month'] = dat['Month'].astype(int)
        dat['Day'] = dat['Day'].astype(int)
        dat = dat.sort_values(by = ['Year', 'Month', 'Day'])
    else:
        dat = pd.DataFrame({'Time': [], 'Time_valid': [], 'Money': [], 'Share': [], 'Bonus': [], 'Stock': []})
    return dat 


class DividendVS():
    def __init__(self,dict_path_) -> None:
        self.dict_path_ = dict_path_
        pass
    def Dividend_VS(self,sym):
        share = get_share(f'{self.dict_path_["F0"]["BonusShare"]}/{sym}.csv').drop_duplicates()
        cash = get_cash(f'{self.dict_path_["F0"]["CashDividend"]}/{sym}.csv').drop_duplicates()
        bonus = get_bonus(f'{self.dict_path_["F0"]["StockDividend"]}/{sym}.csv').drop_duplicates()
        stock = pd.merge(share, bonus, on = ['Time'],how = 'outer')
        stock = stock.fillna('NAN')
        dat = pd.merge(cash, stock, on = ['Time'], how = 'outer')
        dat = dat.fillna('NAN')
        dat = sort_by_time(dat)
        dat = dat[dat['Time'] != 'NAN']
        # chang formal for all cols
        if dat.shape[0] > 0:
            dat['Money'] = dat['Money'].apply(lambda x : change_money2percent(x))
            dat['Share'] = dat['Share'].apply(lambda x : change_stock2(x))
            dat['Bonus'] = dat['Bonus'].apply(lambda x : change_stock2(x))
            dat['Money'] = dat['Money'].apply(lambda x : change_type_all(x))
            dat['Share'] = dat['Share'].apply(lambda x : change_type_all(x))
            dat['Bonus'] = dat['Bonus'].apply(lambda x : change_type_all(x))
        else:
            dat = pd.DataFrame({
                'Time': [], 'Money': [], 'Share': [], 'Bonus': []
            })
        pivot = pd.pivot_table(data = dat,index = ['Time'],
                            aggfunc = {
                                'Money': lambda x : cal_fraction_money(x), 
                                'Share': lambda x : cal_fraction_stock(x),
                                'Bonus': lambda x : cal_fraction_stock(x)
                            })
        pivot = pivot.reset_index()
        pivot['Stock'] = pivot.apply(lambda x : merge_bonus_share(x['Bonus'], x['Share']), axis = 1)
        datetime = sort_by_time2(pivot)
        datetime = datetime[['Time_valid', 'Money', 'Stock']]
        datetime.columns = ['Time', 'Money', 'Stock']
        # loc nan nan 
        t = loc_nan_nan(datetime)
        t["Time"] = t["Time"].apply(lambda row: formatDate(row))
        return t