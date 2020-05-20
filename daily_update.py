import parse_stock_data
import calculate_stock_spec
import datetime
import os
import baostock as bs
import calculate_hs300_spec

#baostock data maybe not update right after the trade market close. 
#It is better to run the daily task after 20:00
if __name__=='__main__':
    current_date = datetime.date.today().strftime('%Y-%m-%d')
    hour = datetime.datetime.now().hour
    
    if hour < 20:
        dd = datetime.date.today() + datetime.timedelta(-1)
        current_date = dd.strftime('%Y-%m-%d')
    
    folderpath = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(folderpath, "date.txt")
    if os.path.exists(file_path):
        with open(file_path,"r") as f:
            start_date = f.read()
    else:
        start_date=current_date
    if start_date == '':
        start_date=current_date

    data_list = []
    bs.login()
    rs = bs.query_trade_dates(start_date=start_date, end_date=current_date)
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    bs.logout()
    lastTradeDate = current_date
    for i in range(len(data_list)-1,-1,-1):
        if data_list[i][1] == '1':
            lastTradeDate = data_list[i][0]
            break
    
    print(start_date, lastTradeDate)
    if start_date <= lastTradeDate:
        print("start to refresh all stock")
        parse_stock_data.refresh_all_stock(lastTradeDate)
        print("start to refresh all stock day K")
        parse_stock_data.refresh_all_stock_day_k(start_date, lastTradeDate)
        print("start to calculate sh")
        calculate_stock_spec.calculate_all_spec('sh', 'sh.000001')
        print("start to calculate sz")
        calculate_stock_spec.calculate_all_spec('sz', 'sz.399001')
        print("start to calculate hs300")
        calculate_hs300_spec.calculate_hs300_spec()
        with open(file_path,"w") as f:
            f.write(current_date) 
