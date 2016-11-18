from bs4 import BeautifulSoup
import requests
import sys
import os
from datetime import datetime
from datetime import date
import pandas as pd
import time

def download_earning_history(ticker):
	print('Downloading earning history for {}'.format(ticker))

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
			line = line + tds[5].text.strip()


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
	print('Downloading: {}'.format(ticker))
	download_price_history(ticker)
	download_earning_history(ticker)

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("download -p ticker | -e ticker")
		print("\t -p download price history")
		print("\t -e download earning history")

	if sys.argv[1] == '-p':		
		for ticker in sys.argv[2:]:
			download_price_history(ticker)

	elif sys.argv[1] == '-e':
		for ticker in sys.argv[2:]:
			download_earning_history(ticker)

	else:
		print("download -p ticker | -e ticker")
		print("\t -p download price history")
		print("\t -e download earning history")
