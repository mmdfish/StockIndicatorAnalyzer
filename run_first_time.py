import parse_stock_data
import calculate_stock_spec
import datetime
import os
import baostock as bs
import createDB
import calculate_hs300_spec

if __name__=='__main__':

    #createDB.createDB()

    current_date = datetime.date.today().strftime('%Y-%m-%d')
    hour = datetime.datetime.now().hour
    #baostock data maybe not update right after the trade market close.
    if hour < 20:
        dd = datetime.date.today() + datetime.timedelta(-1)
        current_date = dd.strftime('%Y-%m-%d')
    
    start_date = "2006-01-01"

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
        print("start to refresh all stock adjust factor")
        parse_stock_data.refresh_all_stock_adjust(start_date, lastTradeDate)
        print("start to refresh all stock day K no adjust")
        parse_stock_data.refresh_all_stock_day_k_no_adjust_first_time(start_date, lastTradeDate)
        print("start to refresh all stock day K")
        parse_stock_data.refresh_all_stock_day_k_first_time(start_date, lastTradeDate)
        print("start to calculate sh")
        calculate_stock_spec.calculate_all_spec('sh', 'sh.000001')
        print("start to calculate sz")
        calculate_stock_spec.calculate_all_spec('sz', 'sz.399001')
        print("start to calculate hs300")
        calculate_hs300_spec.calculate_hs300_spec()

        folderpath = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(folderpath, "date.txt")
        with open(file_path,"w") as f:
            f.write(current_date) 