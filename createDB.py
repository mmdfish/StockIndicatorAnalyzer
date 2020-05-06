import baostock as bs
import pandas as pd
import sqlite3

def createDB():
    conn = sqlite3.connect("mystock.db")
    cursor = conn.cursor()

    # create three tables, allstock stock_day_k and stock_spec
    sql_stock = "CREATE TABLE allstock(code TEXT PRIMARY KEY, tradeStatus INT not null, code_name TEXT not null)"
    sql_day_K = "CREATE TABLE stock_day_k(date date, code TEXT, open REAL, high REAL, low REAL, close REAL,"\
        "preclose REAL, volume INT, amount INT, adjustflag INT, turn REAL, tradestatus INT,"\
            "pctChg REAL, peTTM REAL, pbMRQ REAL, psTTM REAL, pcfNcfTTM REAL, isST INT, primary key (date, code))"
    sql_stock_spec = "CREATE TABLE stock_spec(date date, code TEXT, name TEXT, relacode TEXT, alpha_y REAL, beta_y REAL, r_y REAL,"\
        "alpha_m REAL, beta_m REAL, r_m REAL, corr_y REAL, cov_y REAL, corr_m REAL, cov_m REAL,"\
        "amplitude_y REAL, amplitude_m REAL,amplitude_10 REAL,amplitude_5 REAL,highopen_y REAL,highopen_m REAL, primary key (date, code))"
    sql_day_K_index = "CREATE INDEX code_index ON stock_day_k (code)"
    cursor.execute(sql_stock)
    cursor.execute(sql_day_K)        
    cursor.execute(sql_stock_spec)
    cursor.execute(sql_day_K_index)
    cursor.close()
    conn.commit()
    conn.close()

def alterTable():
    conn = sqlite3.connect("mystock.db")
    cursor = conn.cursor()

    sql_stock_spec = "ALTER TABLE stock_spec add column highopen_y REAL"
    cursor.execute(sql_stock_spec)
    sql_stock_spec = "ALTER TABLE stock_spec add column highopen_m REAL"
    cursor.execute(sql_stock_spec)
    cursor.close()
    conn.commit()
    conn.close()

if __name__=='__main__':
    createDB()
    #alterTable()