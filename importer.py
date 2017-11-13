import os
from stock import *
import sys
from os.path import isfile, join
from datetime import datetime
from datetime import date
import time
import dao

def strfftime(s, f1, f2):
	return time.strftime(f2, time.strptime(s, f1))

def import_all_prices():
	for stock in Stock.list():
		ticker = stock['ticker'] 
		file_name = 'data/' + ticker + '.price.csv'
		if(isfile(file_name)):
			print('Importing {} prices from {}'.format(ticker, file_name))
			df = pd.read_csv(file_name)	
			df = df.rename(columns={'Date'   : 'date',
								   'Open'   : 'open', 
								   'High'   : 'high',
								   'Low'    : 'low',
								   'Close'  : 'close',
								   'Volume' : 'volume'})

			df['date'] = df.apply(lambda row : strfftime(row['date'], '%Y-%m-%d', '%Y%m%d'), axis=1)
			print(df)
			dao.save_price_history(ticker, df.T.to_dict().values())

def import_all_earnings():
	for stock in Stock.list():
		ticker = stock['ticker'] 
		file_name = 'data/' + ticker + '.earning.csv'
		if(isfile(file_name)):
			print('Importing {} prices from {}'.format(ticker, file_name))
			df = pd.read_csv(file_name)	
			print(df)
			dao.save_earning_history(ticker, df.T.to_dict().values())