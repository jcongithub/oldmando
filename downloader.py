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

pd.options.display.width = 1000

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
			'ticker'  : ticker,
			'date'    : tds[2].text.strip(),
			'month'   : tds[3].text.strip(),
			'eps'     : tds[4].text.strip(),
			'numests' : tds[5].text.strip(),
			'last_year_date' : tds[6].text.strip(),
			'last_year_eps' : tds[7].text.strip(),
		}

		records.append(record)

	df = pd.DataFrame(records)
	print(df)
	return df['ticker'].tolist()



def download_earning_history(ticker):
	print('downloading earning history for {}'.format(ticker))

	base_url = 'http://zacks.thestreet.com/tools/earnings_announcements_company.php'
	params = {'ticker':ticker, 'recordsToDisplay':100, 'recordsPerPage':100}
	headers = {'Content-Type':'application/json'}

	response = requests.get(base_url, params=params)
	txt = response.text

	soup = BeautifulSoup(txt, 'html.parser')
	tables = soup.find_all('table')
	if(len(tables) < 3):
		print("\tNo earning history data found")
		return

	trs = tables[2].find_all('tr')

	file_path = 'data/' + ticker + '.earning.csv'
	os.makedirs(os.path.dirname(file_path), exist_ok=True)
	f = open(file_path, 'w')
	for row in range(len(trs)):
		tds = trs[row].find_all('td')
		
		line = ''
		if row == 0:
			for i  in range(len(tds) - 1):						
				line = line + tds[i].text.strip() + ','
			line = line + tds[len(tds)-1].text.strip()			


		else:
			text = tds[0].text.strip()
			line = line + datetime.strptime(text, '%d-%b-%y').date().isoformat() + ','
			line = line + tds[1].text.strip() + ","
			line = line + tds[2].text.strip() + ','
			line = line + tds[3].text.strip() + ','
			line = line + tds[4].text.strip() + ','
			line = line + tds[5].text.strip().replace(',', '')


		f.write(line + "\n")
	
	f.close()



def download_price_history(ticker):
	print('downloading price history: {}'.format(ticker))
	today = datetime.now()
	url = 'http://chart.finance.yahoo.com/table.csv?a=0&b=1&c=1962&g=d&ignore=.csv'
	params = {'s' : ticker,
			  'd' : today.month - 1,
			  'e' : today.day,
			  'f' : today.year}

	response = requests.get(url, params=params)

	file_path = 'data/' + ticker + '.price.csv'
	content_to_file(file_path, response.text) 

def content_to_file(file_path, content):
	os.makedirs(os.path.dirname(file_path), exist_ok=True)
	with open(file_path, "w") as f:
		f.write(content)

def download_history(ticker):
	download_price_history(ticker)
	download_earning_history(ticker)

def download_sp500_stoct_list():
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

	return list;



if __name__ == '__main__':
#	list = download_earning_schedule('2016-Dec-17')
#	print("tickes:{}".format(list))
	list = download_sp500_stoct_list()
	print(list)