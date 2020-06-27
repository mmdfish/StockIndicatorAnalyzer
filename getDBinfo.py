import sqlite3
import common

conn = sqlite3.connect(common.db_path_sqlite3)
cursor = conn.cursor()
cursor.execute('PRAGMA table_info(allstock)')
print(cursor.fetchall())

cursor.execute('PRAGMA table_info(stock_day_k)')
print(cursor.fetchall())

cursor.execute('PRAGMA table_info(stock_spec)')
print(cursor.fetchall())

cursor.execute('PRAGMA table_info(stock_hs300_spec)')
print(cursor.fetchall())

cursor.execute('PRAGMA table_info(stock_qualification)')
print(cursor.fetchall())

cursor.close()
conn.close()