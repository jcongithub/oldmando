import sqlite3
import pandas as pd
from os.path import isfile, join

pd.options.display.width = 1000

conn = sqlite3.connect('data/load')
cur = conn.cursor()

conn2 = sqlite3.connect('data/test_trades')
cur2 = conn2.cursor()

def price(ticker):
	cur.execute("select date,open,high,low,close,volume,adj_close from price where ticker=:ticker order by date desc", {'ticker' : ticker})
	prices = pd.DataFrame(cur.fetchall(), columns=['date','open','high','low','close','volume','adj_close'])
	return prices.set_index(['date'])
	
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
	print("\tSaving {} trades".format(len(trades)))	

	cur2.execute("delete from trades where ticker=:ticker", {'ticker' : ticker})

	for index, trade in trades.iterrows():
		fields = trade.tolist()
		fields.insert(0, ticker)
		print(fields)
		cur2.execute("insert or replace into trades values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", fields)

	conn2.commit();
	print("\tSaved")