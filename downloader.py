from bs4 import BeautifulSoup
import requests
import sys
import os
from datetime import datetime
import pandas as pd
import time
import pandas as pd
import numpy as np
import re
from datetime import date
from os.path import isfile, join


pd.options.display.width = 1000

def trade_file_name(ticker):
	return 'data/' + ticker.lower() + '.trade.csv'

def price_file_name(ticker):
	return 'data/' + ticker.lower() + '.price.csv'

def earning_file_name(ticker):
	return 'data/' + ticker.lower() + '.earning.csv'

def price(ticker):
	return read_history_data(price_file_name(ticker))

def earning(ticker):
	file_name = earning_file_name(ticker)
	return read_history_data(file_name)

def trade(ticker):
	return read_history_data(trade_file_name(ticker))	

def sp500():
	file_name = 'data/sp500.csv'
	if(not isfile(file_name)):
		download_sp500_company_list()

	return pd.read_csv(file_name, index_col=['ticker'])

def read_history_data(file_name):
	history = None
	if(isfile(file_name)):
		history = pd.read_csv(file_name, index_col=['date'])
	else:
		print("History data not found at {}".format(file_name))
	return history

def history_df_header(e):
	if(len(e) > 0):
		return 'records:{} from:{} to:{}'.format(len(e), e.tail(1).index[0], e.head(1).index[0])
	else:
		return 'records:0'

def merge_df(p1, p2):
	if p2 == None:
		return p1
	if len(p2) == 0:
		return p1

	return p1.merge(p2, on=p1.columns.tolist(), left_index=True, right_index=True, how='outer')
	

#####################################################################################################
##   Download stock price history, stock earnings history
def download_earning(tickers):
	for ticker in tickers:
		print('Downloading earning history: {}'.format(ticker))
		base_url = 'http://client1.zacks.com/demo/zackscal/tools/earnings_announcements_company.php'
		params = {'ticker'           : ticker,
				  'pg_no'            : 1,
				  'recordsToDisplay' : 100,
				  'maxNoOfPages'     : 10,
				  'recordsPerPage'   : 25,
				  'showAllFlag'      :'yes'}

		response = requests.get(base_url, params=params)
		text = response.text	
		soup = BeautifulSoup(text, 'html.parser')
		divs = soup.find_all('div', {'id' : 'divPrint'})
		records = []
		if(len(divs) > 0):
			tables = divs[0].find_all('table')
			if(len(tables) > 1):
				trs = tables[1].find_all('tr')

				for i in range(1, len(trs)):
					tds = trs[i].find_all('td')
					record = {
						'date'      : datetime.strptime(tds[0].text.strip(), '%d-%b-%y').date().isoformat(),
						'period'    : tds[1].text.strip(),
						'estimate'  : tds[2].text.strip(),
						'reported'  : tds[3].text.strip(),
						'surprise1' : tds[4].text.strip(),
						'surprise2' : tds[4].text.strip(),
					}
					records.append(record)

		e1 = pd.DataFrame(records).set_index(['date'])
		e2 = earning(ticker)
		
		em = merge_df(e1, e2)
		em.to_csv(earning_file_name(ticker))


def download_price(tickers):
	error_tickers = []
	for ticker in tickers:
		file_path = 'data/' + ticker + '.price.tmp'
		price_file = price_file_name(ticker) 
		try:
			print('Downloading price history: {}'.format(ticker))
			today = datetime.now()
			url = 'http://chart.finance.yahoo.com/table.csv?a=0&b=1&c=1962&g=d&ignore=.csv'
			params = {'s' : ticker,
					  'd' : today.month - 1,
					  'e' : today.day,
					  'f' : today.year}

			response = requests.get(url, params=params)

			
			content_to_file(file_path, response.text)
			
			p = pd.read_csv(file_path)
			p.columns = [x.lower() for x in p.columns]
			p = p.set_index(['date'])
			p2 = price(ticker)
			pm = merge_df(p, p2)	
			pm.to_csv(price_file)

			os.remove(file_path)
		except:
			error_tickers.append(ticker)
			if(isfile(price_file)):
				os.remove(price_file)
				
		if(isfile(file_path)):
			os.remove(file_path)
				

def content_to_file(file_path, content):
	os.makedirs(os.path.dirname(file_path), exist_ok=True)
	with open(file_path, "w") as f:
		f.write(content)

def download_earning_schedule(date):
	base_url = 'http://www.nasdaq.com/earnings/earnings-calendar.aspx'
	params = {'date' : date}
	response = requests.get(base_url, params=params)
	txt = response.text	
	soup = BeautifulSoup(txt, 'html.parser')
	tables = soup.find_all('table', {'id' : 'ECCompaniesTable'})
	if(len(tables) == 0):
		return {};

	trs = tables[0].find_all('tr')

	records = []
	for row in range(1, len(trs)):
		tr = trs[row]
		tds = tr.find_all('td')
		
		company = tds[1].text.strip()
		m = re.match('.*\(([A-Z].*)\).*', company)
		ticker = m.group(1) 

		record = {
			'company' : company,
			'ticker'  : ticker.lower(),
			'date'    : tds[2].text.strip(),
			'month'   : tds[3].text.strip(),
			'eps'     : tds[4].text.strip(),
			'numests' : tds[5].text.strip(),
			'last_year_date' : tds[6].text.strip(),
			'last_year_eps' : tds[7].text.strip(),
		}

		records.append(record)

	df = pd.DataFrame(records)
	return df

def download_sp500_company_list():
	url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
	response = requests.get(url)
	txt = response.text	
	soup = BeautifulSoup(txt, 'html.parser')
	tables = soup.find_all('table')
	trs = tables[0].find_all("tr")
	print(len(trs))
	print(trs[0])

	list = [];

	for i in range(1, len(trs)):
		tds = trs[i].find_all("td")
		ticker = tds[0].text.strip()
		security = tds[1].text.strip()
		sector = tds[3].text.strip()
		industry = tds[4].text.strip()

		print("{} | {} | {} | {}".format(ticker, security, sector, industry))

		company = {'ticker':ticker, 'security':security, 'sector':sector, 'industry':industry}
		list.append(company)

	df = pd.DataFrame(list)
	df['ticker'] = df['ticker'].apply(lambda x : x.lower())
	df = df[df['ticker'].apply(lambda x : x.isalpha())]
	df.to_csv('data/sp500.csv', index=False)
	return df;
