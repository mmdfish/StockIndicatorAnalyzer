import sqlite3
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import common

def testAllStock():
    sql_cmd = "SELECT * FROM allstock"
    cx = sqlite3.connect(common.db_path_sqlite3)
    result = pd.read_sql(sql=sql_cmd, con=cx)
    print(result)
    print(result.shape[0])

def testDayK():
    #sql_cmd = "SELECT * FROM stock_day_k"
    sql_cmd = "SELECT * FROM stock_day_k where code='sh.000001' order by date asc limit 0,10"
    #sql_cmd = "SELECT * FROM stock_day_k where date='2020-03-05'"
    #sql_cmd = "SELECT * FROM stock_day_k where code='sh.000001' and date>'2019-01-01'"
    begin = datetime.now()
    cx = create_engine(common.db_path_sqlalchemy)
    begin2 = datetime.now()
    elapse = begin2 - begin
    print(elapse)
    result = pd.read_sql(sql=sql_cmd, con=cx)
    elapse = datetime.now() - begin2
    print(elapse)
    print(result)

def testOneStock():
    ticker = 'sh.688126'
    cx = create_engine(common.db_path_sqlalchemy)
    sql_cmd = "SELECT count(*) FROM stock_day_k where code='" + ticker + "'"
    cursor = cx.execute(sql_cmd)
    result = cursor.fetchone()
    dayk_count = 365
    if result[0] < 365:
        dayk_count = result[0]
    sql_cmd = "SELECT * FROM stock_day_k where code='" + ticker +"' order by date desc limit 0," + str(dayk_count)
    print(sql_cmd)
    begin = datetime.now()
    begin2 = datetime.now()
    elapse = begin2 - begin
    print(elapse)
    result = pd.read_sql(sql=sql_cmd, con=cx)
    elapse = datetime.now() - begin2
    print(elapse)
    print(result)

def testStockSpec():
    cx = create_engine(common.db_path_sqlalchemy)
    #sql_cmd = "SELECT * FROM stock_spec where code='sh.688358'"
    sql_cmd = "SELECT * FROM stock_spec where date=(select max(date) from stock_spec) and amplitude_10 > '0' order by amplitude_10 asc limit 0,50"
    #sql_cmd = "SELECT * FROM stock_spec where date=(select max(date) from stock_spec) order by amplitude_10 asc limit 0,50"
    
    result = pd.read_sql(sql=sql_cmd, con=cx)
    print(result)

def testDate():
    cx = create_engine(common.db_path_sqlalchemy)
    sql_cmd = "select max(date) from stock_spec"
    cursor = cx.execute(sql_cmd)
    result = cursor.fetchone()
    print(result[0])

def testSearchTime():
    begin = datetime.now()
    ticker = 'sh.688126'
    cx = create_engine(common.db_path_sqlalchemy)
    sql_cmd = "SELECT count(*) FROM stock_day_k where code='" + ticker + "'"
    cursor = cx.execute(sql_cmd)
    result = cursor.fetchone()
    # dayk_count = 365
    # if result[0] < 365:
    #     dayk_count = result[0]

    elapse = datetime.now() - begin
    print(result[0], elapse)
#testAllStock()
testDayK()
#testOneStock()
#testStockSpec()
#testDate()
#testSearchTime()