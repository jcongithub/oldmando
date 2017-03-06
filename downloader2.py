import sqlite3
import pandas as pd
from os.path import isfile, join
import mpf

pd.options.display.width = 1000

conn = sqlite3.connect('data/load')
cur = conn.cursor()

conn2 = sqlite3.connect('data/test_trades')
cur2 = conn2.cursor()

def price1(ticker, date=None):
	mpf.task_start("QueryPrice")
	if(date is None):
		cur.execute("select date,open,high,low,close,volume,adj_close from price where ticker=:ticker order by date desc", {'ticker' : ticker})
	else:
		cur.execute("select date,open,high,low,close,volume,adj_close from price where ticker=:ticker and date=:date order by date desc", 
			{'ticker' : ticker, 'date': date})
	mpf.task_end("QueryPrice")
	return cur.fetchall()

def price(ticker, date=None):
	p = price1(ticker, date)
	mpf.task_start("CreatePriceDataFrame")
	prices = pd.DataFrame(p, columns=['date','open','high','low','close','volume','adj_close'])
	prices = prices.set_index(['date'])
	mpf.task_end("CreatePriceDataFrame")
	return prices;
	
def earning(ticker):
	cur.execute("select date,estimate,period,reported,surprise1,surprise2 from earning where ticker=:ticker order by date desc", {'ticker' : ticker})
	prices = pd.DataFrame(cur.fetchall(), columns=['date','estimate','period','reported','surprise1','surprise2'])
	return prices.set_index(['date'])

def sp500():
	file_name = 'data/sp500.csv'
	if(not isfile(file_name)):
		download_sp500_company_list()

	return pd.read_csv(file_name, index_col=['ticker'])

def save_test_trades(ticker, trades):
	trades.reset_index(inplace=True)
	cur2.execute("delete from trades where ticker=:ticker", {'ticker' : ticker})

	for index, trade in trades.iterrows():
		fields = trade.tolist()
		fields.insert(0, ticker)
		cur2.execute("insert or replace into trades values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", fields)

	conn2.commit();


def find_periods_tested():
	cur2.execute("select distinct period from trades order by period")
	periods = cur2.fetchall()
	return [period[0] for period in periods]

def find_profitable_stock_ratio_for_period(periods):
	result = [];
	for period in periods:
		cur2.execute("select count(*) from (select distinct ticker from trades where period=:period)", {"period" : period})
		total_stocks = cur2.fetchone()

		cur2.execute("select count(*) from (select distinct ticker from trades where period=:period and profit2 > 0)", {"period" : period})
		profitable_stocks = cur2.fetchone();

		result.append({'period' : period, 'count' : total_stocks[0], 'winning' : profitable_stocks[0]})

	return result

def find_profitable_stocks_for_period(period):
	cur2.execute("select distinct ticker from trades where profit2 > 0 and period =:period", {"period" : period})
	profitable_stocks = cur2.fetchall()
	return [stock[0] for stock in profitable_stocks]


def find_consective_winning_stocks(start_period, num_period):
	QUARTERS = ['Mar', 'Jun', 'Sep', 'Dec']
	quarter = start_period[:3]
	iquarter = QUARTERS.index(quarter)
	year = int(start_period[4:])

	periods = []
	for i in range(num_period):
		if iquarter >= 4:
			iquarter = 0
			year = year + 1
		
		period = QUARTERS[iquarter] + ' ' + str(year)
		periods.append(period)

		iquarter = iquarter + 1	

	print(periods)

	for period in periods:
		print (period)
		stocks = find_profitable_stocks_for_period(period)
		print(stocks)
		print(len(stocks))
		print("")


