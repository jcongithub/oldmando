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
from datetime import timedelta
from os.path import isfile, join
from os import listdir
import fileinput
import sqlite3


pd.options.display.width = 1000

def trade_file_name(ticker):
	return 'data/' + ticker.lower() + '.trade.csv'

def price_file_name(ticker):
	return 'data/' + ticker.lower() + '.price.csv'

def earning_file_name(ticker):
	return 'data/' + ticker.lower() + '.earning.csv'

def price(ticker):
	return read_history_data(price_file_name(ticker))

def earnings(ticker_list):
	today = datetime.now().strftime('%Y-%m-%d')
	s = pd.DataFrame()

	for ticker in ticker_list:
		eh = earning(ticker);

		edate = eh.index[0]
		if(edate > today):
			s = s.append({'ticker' : ticker, 'edate' : edate}, ignore_index=True)

	return s

def earning(ticker, start_year='2000', num_years=100):
	file_name = earning_file_name(ticker)
	e = read_history_data(file_name)
	result = pd.DataFrame()
	
	if e is not None:
		e['period2'] = e.apply(lambda row : row['period'][4:], axis=1)
		start = int(start_year)


		if(num_years > 0):
			start = start
			end = start + num_years
		else:
			end = start + 1
			start = start + num_years + 1

		for year in range(start, end):
			a = e[e['period2'] == str(year)]
			result = result.append(a)

		result.drop('period2', inplace=True, axis=1)
		result.sort_index(ascending=False)

	return result


def testtrade(ticker, earning_date_list=None):
	file_name = trade_file_name(ticker)
	history = None
	if(isfile(file_name)):
		history = pd.read_csv(file_name, index_col=['earning_date'])
	else:
		print("No test trade file found at {}".format(file_name))

	if(earning_date_list is None):
		return history

	filtered = pd.DataFrame()
	index = history.index.tolist()
	for earning_date in earning_date_list:
		if earning_date in index:
			filtered = filtered.append(history.loc[earning_date])

	return filtered


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


def merge_df(p1, p2):
	if p2 is None:
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
	skipped = []
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

		except:
			skipped.append(ticker)
			if(isfile(price_file)):
				os.remove(price_file)
				
		if(isfile(file_path)):
			os.remove(file_path)
	
	if(len(skipped) > 0):
		print("Cannot download price history data for following tickerss")
		print(skipped)
				

def content_to_file(file_path, content):
	os.makedirs(os.path.dirname(file_path), exist_ok=True)
	with open(file_path, "w") as f:
		f.write(content)

def download_earning_schedule(start_date=datetime.now(), number_days = 1, update_database = True):
	for i in range(number_days):
		date = start_date + timedelta(days = i)
		sdate = date.strftime('%Y-%b-%d')
		print(sdate)

		base_url = 'http://www.nasdaq.com/earnings/earnings-calendar.aspx'
		params = {'date' : sdate}
		response = requests.get(base_url, params=params)
		txt = response.text	
		soup = BeautifulSoup(txt, 'html.parser')
		tables = soup.find_all('table', {'id' : 'ECCompaniesTable'})

		if(len(tables) > 0):
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

			print("{} companies on {}".format(len(records), sdate))			
			save_earning_schedule(records)
		else:
			print("downloading faild")	

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

def load_earning_data():
	conn = sqlite3.connect('load')
	cur = conn.cursor()

	for file in listdir('.'):
		if 'earning.csv' in file:
			ticker = file[:-12]
			print("file:{} ticker:{}".format(file, ticker))
			with open(file) as f:
				next(f)
				for line in f:
					line = line.rstrip('\n')
					fields = line.split(",")
					fields.insert(0, ticker)
					try:
						cur.execute("insert or replace into earning values(?, ?, ?, ?, ?, ?, ?)", fields[:-1])
					except:
						print("{} - {}".format(fields, sys.exc_info()))

	cur.execute("select count(*) from earning")
	print(cur.fetchall())

	conn.commit()
	conn.close()	

def load_price_data():
	conn = sqlite3.connect('load')
	cur = conn.cursor()

	for file in listdir('.'):
		if 'price.csv' in file:
			ticker = file[:-10]
			print("file:{} ticker:{}".format(file, ticker))
			with open(file) as f:
				next(f)
				for line in f:
					line = line.rstrip('\n')
					fields = line.split(",")
					fields.insert(0, ticker)
					try:
						cur.execute("insert or replace into price values(?, ?, ?, ?, ?, ?, ?, ?)", fields)
					except:
						print("{} - {}".format(fields, sys.exc_info()))

	cur.execute("select count(*) from price")
	print(cur.fetchall())

	conn.commit()
	conn.close()	

def save_earning_schedule(list_schedule):
	conn = sqlite3.connect('./db/schedule')
	cur = conn.cursor()
	for schedule in list_schedule:
		cur.execute("insert or replace into schedule(ticker, date, eps, last_year_date, last_year_eps, month, numests, company ) values(:ticker,:date, :eps, :last_year_date, :last_year_eps, :month, :numests, :company)", schedule)

	conn.commit()
	conn.close()	

#if __name__ == '__main__':
#	load_price_data()
#	load_earning_data()	