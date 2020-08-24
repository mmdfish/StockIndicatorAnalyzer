import redis
import common
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import json 

# re = redis.Redis(host='127.0.0.1', port=6379,db=0)
# re.set('key_name','value_tom')
# print(re.get('key_name'))

def dayk(codename='sh.000001'):
    cx = create_engine(common.db_path_sqlalchemy)
    sql_cmd = "SELECT * FROM stock_day_k where code='" + codename +"' order by date desc limit 0,5"
    
    result = pd.read_sql(sql=sql_cmd, con=cx)

    df_json = result.to_json(orient = 'table', force_ascii = False)
    #print(df_json)
    #data = json.loads(df_json)
    return df_json
    # print(data)

if __name__=='__main__':
    re = redis.Redis(host='127.0.0.1', port=6379,db=0)
    re.set('key_name','value_tom')
    print(re.get('key_name'))
    
    codename = 'sh.000001'
    if re.exists(codename) == 0:
        value = dayk(codename)
        re.set(codename,value)
        print('insert')
    if re.exists(codename) == 1:
        print(re.get(codename))    

