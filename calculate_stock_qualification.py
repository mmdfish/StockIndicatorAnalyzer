import logging
from stockstats import StockDataFrame as Sdf
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import common

# macd_cross_above macd_cross_below cross_above_boll cross_below_boll
def cal_macd_boll(dayK):
    logging.disable(logging.CRITICAL)
    if dayK.shape[0] < 4:
        return 0,0,0,0
    dailylastN = dayK
    if dayK.shape[0] > 65:
        dailylastN = dayK.tail(65)
    _stock = Sdf.retype(dailylastN) 
    _stock.get('macd')
    _stock.get('boll')
    macd_cross_above = 0
    macd_cross_below = 0
    cross_above_boll = 0
    cross_below_boll = 0
    if(((_stock['macd'][-3] <= _stock['macds'][-3]) & (_stock['macd'][-2] >= _stock['macds'][-2]))
        | ((_stock['macd'][-2] <= _stock['macds'][-2]) & (_stock['macd'][-1] >= _stock['macds'][-1]))):
        macd_cross_above = 1
    if(((_stock['macd'][-3] >= _stock['macds'][-3]) & (_stock['macd'][-2] <= _stock['macds'][-2]))
        | ((_stock['macd'][-2] >= _stock['macds'][-2]) & (_stock['macd'][-1] <= _stock['macds'][-1]))):
        macd_cross_below = 1
    
    if((_stock['high'][-3] > _stock['boll_ub'][-3]) | (_stock['high'][-2] > _stock['boll_ub'][-2]) | (_stock['high'][-1] > _stock['boll_ub'][-1])):
        cross_above_boll = 1
    if((_stock['low'][-3] < _stock['boll_lb'][-3]) | (_stock['low'][-2] < _stock['boll_lb'][-2]) | (_stock['low'][-1] < _stock['boll_lb'][-1])):
        cross_below_boll = 1
    return macd_cross_above,macd_cross_below,cross_above_boll,cross_below_boll

# cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10, ma5_cross_up_ma10, 
# ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, ma5_cross_up_ma30, 
# ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 
def cal_ma_spec(dayKKK):
    cross_down_ma5 = 0
    cross_up_ma5 = 0
    ma5_5 = 0
    ma5_10 = 0
    cross_down_ma10 = 0
    cross_up_ma10 = 0
    ma5_cross_down_ma10 = 0
    ma5_cross_up_ma10 = 0
    ma5_cross_down_ma20 = 0
    ma5_cross_up_ma20 = 0
    ma5_cross_down_ma30 = 0
    ma5_cross_up_ma30 = 0 
    ma5_cross_down_ma60 = 0
    ma5_cross_up_ma60 = 0
    ma20_cross_down_ma60 = 0
    ma20_cross_up_ma60 = 0

    dailylastN = dayKKK
    if dayKKK.shape[0] > 65:
        dailylastN = dayKKK.tail(65)
    dayK = Sdf.retype(dailylastN) 
    length = dayK.shape[0] 
    if(length < 6):
        return cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10,\
             ma5_cross_up_ma10, ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, \
                 ma5_cross_up_ma30, ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 
    dayK['ma5'] = dayK.close.rolling(5).mean()
    if((dayK['close'][-1] < dayK['ma5'][-1]) & (dayK['close'][-2] > dayK['ma5'][-2])):
        cross_down_ma5 = 1
    if((dayK['close'][-1] > dayK['ma5'][-1]) & (dayK['close'][-2] < dayK['ma5'][-2])):
        cross_up_ma5 = 1
    if(length < 11):
        return cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10,\
             ma5_cross_up_ma10, ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, \
                 ma5_cross_up_ma30, ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 
    ma5_5 = ma_desc_or_asc(dayK['ma5'],5)
    dayK['ma10'] = dayK.close.rolling(10).mean()
    if((dayK['close'][-1] < dayK['ma10'][-1]) & (dayK['close'][-2] > dayK['ma10'][-2])):
        cross_down_ma10 = 1
    if((dayK['close'][-1] > dayK['ma10'][-1]) & (dayK['close'][-2] < dayK['ma10'][-2])):
        cross_up_ma10 = 1
    
    if((dayK['ma5'][-1] < dayK['ma10'][-1]) & (dayK['ma5'][-2] > dayK['ma10'][-2])):
        ma5_cross_down_ma10 = 1
    if((dayK['ma5'][-1] > dayK['ma10'][-1]) & (dayK['ma5'][-2] < dayK['ma10'][-2])):
        ma5_cross_up_ma10 = 1
    if(length > 15):
        ma5_10  = ma_desc_or_asc(dayK['ma5'],10)

    if(length < 21):
        return cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10,\
             ma5_cross_up_ma10, ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, \
                 ma5_cross_up_ma30, ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 
    dayK['ma20'] = dayK.close.rolling(20).mean()
    if((dayK['ma5'][-1] < dayK['ma20'][-1]) & (dayK['ma5'][-2] > dayK['ma20'][-2])):
        ma5_cross_down_ma20 = 1
    if((dayK['ma5'][-1] > dayK['ma20'][-1]) & (dayK['ma5'][-2] < dayK['ma20'][-2])):
        ma5_cross_up_ma20 = 1

    if(length < 31):
        return cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10,\
             ma5_cross_up_ma10, ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, \
                 ma5_cross_up_ma30, ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 
    dayK['ma30'] = dayK.close.rolling(30).mean()
    if((dayK['ma5'][-1] < dayK['ma30'][-1]) & (dayK['ma5'][-2] > dayK['ma30'][-2])):
        ma5_cross_down_ma30 = 1
    if((dayK['ma5'][-1] > dayK['ma30'][-1]) & (dayK['ma5'][-2] < dayK['ma30'][-2])):
        ma5_cross_up_ma30 = 1
    
    if(length < 61):
        return cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10,\
             ma5_cross_up_ma10, ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, \
                 ma5_cross_up_ma30, ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 
    dayK['ma60'] = dayK.close.rolling(60).mean()
    if((dayK['ma5'][-1] < dayK['ma60'][-1]) & (dayK['ma5'][-2] > dayK['ma60'][-2])):
        ma5_cross_down_ma60 = 1
    if((dayK['ma5'][-1] > dayK['ma60'][-1]) & (dayK['ma5'][-2] < dayK['ma60'][-2])):
        ma5_cross_up_ma60 = 1
    if((dayK['ma20'][-1] < dayK['ma60'][-1]) & (dayK['ma20'][-2] > dayK['ma60'][-2])):
        ma20_cross_down_ma60 = 1
    if((dayK['ma20'][-1] > dayK['ma60'][-1]) & (dayK['ma20'][-2] < dayK['ma60'][-2])):
        ma20_cross_up_ma60 = 1
    return cross_down_ma5, cross_up_ma5, ma5_5, ma5_10, cross_down_ma10, cross_up_ma10, ma5_cross_down_ma10,\
             ma5_cross_up_ma10, ma5_cross_down_ma20, ma5_cross_up_ma20, ma5_cross_down_ma30, \
                 ma5_cross_up_ma30, ma5_cross_down_ma60, ma5_cross_up_ma60, ma20_cross_down_ma60, ma20_cross_up_ma60, 

#ma_desc_x 0: no 1:yes 2:asc
def ma_desc_or_asc(ma, dayNumber):
    if len(ma) < dayNumber + 5:
        return 0
    
    ddd = ma.tail(dayNumber)
    if ddd[0] >= ddd[1]:
        for i in range(1, dayNumber - 1):
            if ddd[i] < ddd[i + 1]:
                return 0
        return 1
    if ddd[0] <= ddd[1]:
        for i in range(1, dayNumber - 1):
            if ddd[i] > ddd[i + 1]:
                return 0
        return 2
    return 0 

#huge_volume
def cal_huge_volume(dayKKK):
    if dayKKK['tradestatus'][dayKKK.shape[0] - 1] == 0:
        return 0
    #print(dayK)
    length = dayKKK.shape[0] 
    if(length < 30):
        return 0
    daykLastN = dayKKK.tail(length)
    dayK = Sdf.retype(daykLastN) 
    volume = dayK['volume']
    sum_3 = 0
    count = 0
    for i in range(1,10):
        index = -i
        if isinstance(volume[index],str):
            continue
        if volume[index] == np.nan:
            continue
        if volume[index] == 0:
            continue
        sum_3 += volume[index]
        count = count + 1
        if count == 3:
            break
    if count == 0:
        return 0
    avg_3 = sum_3/count
    
    sum_30 = 0
    count = 0
    for i in range(4,length):
        index = -i
        if isinstance(volume[index],str):
            continue
        if volume[index] == np.nan:
            continue
        if volume[index] == 0:
            continue
        sum_30 += volume[i]
        count = count + 1
        if count == 30:
            break
    avg_30 = sum_30/count
    if(avg_3 > avg_30 * 3):
        #print("放量")
        return 1
    return 0

#dayk_desc_x 0: no 1:yes 2:asc
def dayK_desc_or_asc(dayK, dayNumber):
    if dayK['tradestatus'][dayK.shape[0] - 1] == 0:
        return 0
    if len(dayK) < dayNumber:
        return 0
    dayK = Sdf.retype(dayK) 
    length = dayK.shape[0]
    change = dayK['pctchg']
    if change[length - dayNumber] > 0:
        for i in range(1, dayNumber):
            if change[length - dayNumber + i] < 0:
                return 0
        return 2
    if change[length - dayNumber] < 0:
        for i in range(1, dayNumber):
            if change[length - dayNumber + i] > 0:
                return 0
        return 1
    return 0 

if __name__=='__main__':
    ticker = 'sh.600651'
    db = create_engine(common.db_path_sqlalchemy)
    sql_cmd = "SELECT * FROM stock_day_k where code='" + ticker+"' order by date desc limit 0,251"
    daily = pd.read_sql(sql=sql_cmd, con=db)
    daily = daily.sort_values(by='date', ascending=True)
    daily = daily.reset_index(drop = True)
    print(daily)
    #if daily['tradestatus'][daily.shape[0] - 1] == 0:
    #    exit(0)
    dayK_desc_or_asc(daily,3)