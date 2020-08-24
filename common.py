db_path_sqlite3 = r'D:\codes\python\stock\mystock.db'
db_path_sqlalchemy = r'sqlite:///D:\codes\python\stock\mystock.db'

def getAdjust(adjust, date):
    for i in range(adjust.shape[0]):
        if date >= adjust.loc[i,'date']:
            return adjust.loc[i,'foreadjustfactor']
    return 1

def calculateDayKWithAdjustFactor(daily, factor):
    #print(factor)
    #print(daily)
    for i in range(daily.shape[0]):
        forefactor = getAdjust(factor, daily.loc[i,'date'])
        #print(daily.loc[i,'date'], forefactor)
        daily.loc[i,'open'] = forefactor * daily.loc[i,'open']
        daily.loc[i,'high'] = forefactor * daily.loc[i,'high']
        daily.loc[i,'low'] = forefactor * daily.loc[i,'low']
        daily.loc[i,'close'] = forefactor * daily.loc[i,'close']
        daily.loc[i,'preclose'] = forefactor * daily.loc[i,'preclose']