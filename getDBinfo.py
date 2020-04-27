import sqlite3

conn = sqlite3.connect('mystock.db')
cursor = conn.cursor()
cursor.execute('PRAGMA table_info(allstock)')
print(cursor.fetchall())

cursor.execute('PRAGMA table_info(stock_day_k)')
print(cursor.fetchall())

cursor.execute('PRAGMA table_info(stock_spec)')
print(cursor.fetchall())

cursor.close()
conn.close()