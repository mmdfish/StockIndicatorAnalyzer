from pandas import DataFrame
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from scipy import stats
import datetime
import common
import calculate_stock_qualification

def calculate_all_spec(codes, relacode):
    db = create_engine(common.db_path_sqlalchemy)

    sql_cmd = "SELECT * FROM allstock where code like '" + codes + "%'"
    allstocks = pd.read_sql(sql=sql_cmd, con=db)

    number = 0
    result = []
    macdresult = []
    sql_cmd = "SELECT * FROM stock_day_k where code='" + relacode + "' order by date desc limit 0,251"
    datash = pd.read_sql(sql=sql_cmd, con=db)
    price = DataFrame({'date': datash['date'], relacode:datash['close']})
    currentdate = datash['date'][0]
    #realcurrenttime = datetime.date.today().strftime('%Y-%m-%d')
    # if currentdate != realcurrenttime:
    #     return
    datash = datash.sort_values(by='date', ascending=True)
    datash = datash.reset_index(drop = True)
    datash['relaprice'] = datash['close']/datash['close'][0]
    relash = datash['relaprice']
    for (ticker,code_name,tradestatus) in zip(allstocks['code'],allstocks['code_name'],allstocks['tradeStatus']):
        if ticker == relacode:
            continue
        number += 1
        print(number, ticker, code_name)
        sql_cmd = "SELECT * FROM stock_day_k where code='" + ticker+"' order by date desc limit 0,20"
        daily = pd.read_sql(sql=sql_cmd, con=db)
        if(len(daily) == 1):
            continue
        if tradestatus == 0:
            count = 0
            canSkip = False
            for tradestatus2 in daily['tradestatus']:
                if tradestatus2 == 1:
                    break
                count += 1
                if count ==10:
                    canSkip = True
                    break

            if canSkip:
                continue 
        
        sql_cmd = "SELECT * FROM stock_day_k where code='" + ticker+"' and tradestatus='1' order by date desc limit 0,251"
        daily = pd.read_sql(sql=sql_cmd, con=db)

        daily = daily.sort_values(by='date', ascending=True)
        daily = daily.reset_index(drop = True)
        daily['relaprice'] = daily['close']/daily['close'][0]
        relaticker = daily['relaprice']

        tickerresult = []
        tickerresult.append(currentdate)
        tickerresult.append(ticker)
        tickerresult.append(code_name)
        tickerresult.append(relacode)
        alpha,beta,r_value = cal_alpha_beta(relash, relaticker)
        tickerresult.append(alpha)
        tickerresult.append(beta)
        tickerresult.append(r_value)
        alpha,beta,r_value = cal_alpha_beta(relash, relaticker, 20)
        tickerresult.append(alpha)
        tickerresult.append(beta)
        tickerresult.append(r_value)

        pp = DataFrame({ticker:daily['close']})
        price2 = pd.concat([price, pp], axis=1)
        price2 = price2.sort_values(by='date', ascending=True)
        price2 = price2.drop(['date'], axis=1)
        price2 = price2.reset_index(drop = True)
        corr, cov = cal_correlation(price2, pp.shape[0])
        tickerresult.append(corr)
        tickerresult.append(cov)
        corr, cov = cal_correlation(price2, pp.shape[0], 20)
        tickerresult.append(corr)
        tickerresult.append(cov)

        amplitude_year = cal_amplitude(daily)
        amplitude_month = cal_amplitude(daily, 20)
        amplitude_ten = cal_amplitude(daily, 10)
        amplitude_five = cal_amplitude(daily, 5)

        tickerresult.append(amplitude_year)
        tickerresult.append(amplitude_month)
        tickerresult.append(amplitude_ten)
        tickerresult.append(amplitude_five)

        highopen_year = cal_highopen(daily)
        highopen_month = cal_highopen(daily, 20)

        tickerresult.append(highopen_year)
        tickerresult.append(highopen_month)

        result.append(tickerresult)

        if tradestatus == 0:
            continue
        tickermacd = []
        tickermacd.append(currentdate)
        tickermacd.append(ticker)
        tickermacd.append(code_name)
        tickermacd.extend(calculate_stock_qualification.cal_macd_boll(daily))
        tickermacd.extend(calculate_stock_qualification.cal_ma_spec(daily))
        tickermacd.append(calculate_stock_qualification.cal_huge_volume(daily))
        tickermacd.append(calculate_stock_qualification.dayK_desc_or_asc(daily,3))
        oversold = calculate_stock_qualification.dayK_oversold(daily, 60)
        tickermacd.append(oversold)
        macdresult.append(tickermacd)

    if len(result) != 0:  
        db.execute(r'''
        INSERT OR REPLACE INTO stock_spec VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', result)
    
    if len(macdresult) != 0:  
        db.execute(r'''
        INSERT OR REPLACE INTO stock_qualification VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', macdresult)

def cal_alpha_beta(relash, relaticker, dayNumber=0):
    if dayNumber > relaticker.shape[0]:
        return np.nan,np.nan,np.nan
    if dayNumber == 0:
        dayNumber = relaticker.shape[0]
    beta,alpha,r_value,p_value,std_err=stats.linregress(relash[-dayNumber:], relaticker[-dayNumber:])
    return alpha,beta,r_value**2

def cal_correlation(price, length, dayNumber=0):
    if dayNumber >length:
        return np.nan,np.nan
    if dayNumber == 0:
        dayNumber = length
    pricelastN = price.tail(dayNumber)
    returns = pricelastN.pct_change()
    corr=returns.corr()
    cov=returns.cov()
    return corr.iloc[1][0],cov.iloc[1][0]

def cal_amplitude(dayK,dayNumber=0):
    if dayNumber > dayK.shape[0]:
        return np.nan
    if dayNumber == 0:
        dayNumber = dayK.shape[0]
    dailylastN = dayK.tail(dayNumber)
    maxhigh = dailylastN['high'].max()
    minlow = dailylastN['low'].min()
    amplitude = (maxhigh - minlow)/dailylastN['preclose'].iloc[0]
    return amplitude

def cal_highopen(dayK, dayNumber=0):
    if dayNumber > dayK.shape[0]:
        return np.nan
    if dayNumber == 0:
        dayNumber = dayK.shape[0]
    
    number = 0
    dailylastN = dayK.tail(dayNumber)
    for i in range(dayNumber):
        if dailylastN['open'].iloc[i] > dailylastN['preclose'].iloc[i]:
            number += 1
    return number
    
if __name__=='__main__':
    starttime = datetime.datetime.now()
    #calculate_all_spec('sh.600918', 'sh.000001')
    #calculate_all_spec('sh', 'sh.000001')
    calculate_all_spec('sz.300059', 'sz.399001')
    #long running

    endtime = datetime.datetime.now()
    print(endtime - starttime)