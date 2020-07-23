from pandas import DataFrame
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import baostock as bs
import common

def calculate_hs300_spec():
    lg = bs.login()
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    rs = bs.query_hs300_stocks()
    print('query_hs300 error_code:'+rs.error_code)
    print('query_hs300  error_msg:'+rs.error_msg)

    hs300_stocks = []
    while (rs.error_code == '0') & rs.next():
        hs300_stocks.append(rs.get_row_data())
    bs.logout()

    db = create_engine(common.db_path_sqlalchemy)
    hs300code = 'sh.000300'
    sql_cmd = "SELECT * FROM stock_day_k where code='" + hs300code + "' order by date desc limit 0,251"
    datash = pd.read_sql(sql=sql_cmd, con=db)
    price = DataFrame({'date': datash['date'], hs300code:datash['close']})

    result = []
    currentdate = datash['date'][0]
    for stockinfo in hs300_stocks:
        stockcode = stockinfo[1]
        code_name = stockinfo[2]
        sql_cmd = "SELECT * FROM stock_day_k where code='" + stockcode+"' order by date desc limit 0,251"
        daily = pd.read_sql(sql=sql_cmd, con=db)
        pp = DataFrame({stockcode:daily['close']})
        price2 = pd.concat([price, pp], axis=1)
        price2 = price2.drop(['date'], axis=1)
        price2 = price2.reset_index(drop = True)

        tickerresult = []
        tickerresult.append(currentdate)
        tickerresult.append(stockcode)
        tickerresult.append(code_name)
        trendgap_y = calculate_trendgap(price2, pp.shape[0])
        trendgap_hy = calculate_trendgap(price2, pp.shape[0], 120)
        trendgap_qy = calculate_trendgap(price2, pp.shape[0], 60)
        trendgap_m = calculate_trendgap(price2, pp.shape[0], 20)
        trendgap_10 = calculate_trendgap(price2, pp.shape[0], 10)
        trendgap_5 = calculate_trendgap(price2, pp.shape[0], 5)
        tickerresult.append(trendgap_y)
        tickerresult.append(trendgap_hy)
        tickerresult.append(trendgap_qy)
        tickerresult.append(trendgap_m)
        tickerresult.append(trendgap_10)
        tickerresult.append(trendgap_5)
        result.append(tickerresult)
        print(code_name)
    
    if len(result) != 0:  
        db.execute(r'''
        INSERT OR REPLACE INTO stock_hs300_spec VALUES (?,?,?,?,?,?,?,?,?)
        ''', result)
    
        

def calculate_trendgap(price, length, dayNumber=0):
    if dayNumber >length:
        return np.nan
    if dayNumber == 0:
        dayNumber = length
    relaprice1 = price.iloc[0][0]/price.iloc[dayNumber-1][0]
    relaprice2 = price.iloc[0][1]/price.iloc[dayNumber-1][1]
    
    return relaprice2 - relaprice1


if __name__=='__main__':
    calculate_hs300_spec()


