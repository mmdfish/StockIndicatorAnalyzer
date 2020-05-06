# StockIndicatorAnalyzer

用BaoStock来抓取A股的数据并计算一些指标。

[简书](https://www.jianshu.com/p/1eeb120d9eed) [githubpages](https://mmdfish.github.io/2020/04/28/%E7%94%A8BaoStock%E8%8E%B7%E5%BE%97%E6%95%B0%E6%8D%AE%E5%B9%B6%E5%AD%98%E5%85%A5%E6%95%B0%E6%8D%AE%E5%BA%93/)

## 开发环境

IDE: visual studio code

Python 3.6



Requirements:

baostock 0.8.8  ([http://baostock.com/baostock/index.php/](http://baostock.com/baostock/index.php/))

pandas==0.23.4

SQLAlchemy==1.3.13

numpy==1.15.4

scipy==1.1.0



如果pip的源服务器是默认的话可能会很慢，使用淘宝的pip源

pip install -r .\requirements.txt -i https://mirrors.aliyun.com/pypi/simple

使用sqlite3作为数据库存储数据



## 运行

第一次运行的话，运行 run_first_time.py

之后每日更新，运行 daily_update.py，建议在每天晚上八点之后更新，因为大盘收盘后baostock也是会有一定的延迟。



## 计算的指标

上证的股票用上证指数计算，深证的股票用深证指数计算。(未区分基金，创业板和新三板)

当前计算的有贝塔系数，相关性系数，振幅。

年系数用251天，月系数用20天。



# 
