import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

from datetime import datetime
from datetime import timedelta
from os import listdir
from os.path import isfile, join

import glob
from downloader import download_earning_schedule
from downloader import download_sp500_company_list
from downloader import price
from downloader import earning
from downloader import sp500
from downloader import history_df_header
from downloader import trade_file_name
from downloader import download_price
from downloader import download_earning

stock = {}
TODAY = datetime.now().strftime('%Y-%m-%d')
print("TODAY:{}".format(TODAY))

def find_stocks_meet_goal(buy_day_range, sell_day_range, goal):
	good_stocks = []
	files = glob.glob('data/*.trades.csv')
	print("{} numer of stocks".format(len(files)))	
	for file in files:
		meet = find_best_algo_params(file, buy_day_range, sell_day_range, goal)
		if(meet):
			good_stocks.append(file)

	print("{} number of good stocks".format(len(good_stocks)))
	print(good_stocks)

def find_best_algo_params(trades_file, buy_day_range, sell_day_range, goal):
#	test_trades = pd.read_csv('data/' + ticker + '.trades.csv')
	print(trades_file)
	meet = False
	test_trades = pd.read_csv(trades_file)
	
	results = [];
	for sell_days in range(sell_day_range):
		for buy_days in range(buy_day_range):

			trades = test_trades[(test_trades.sell_days==sell_days) & (test_trades.buy_days==-buy_days)]
			
			wining_trades = trades[trades.profit>0]
			num_winings = len(wining_trades)
			
			if(num_winings > goal):
				meet = True				
				mean_profit = (wining_trades['profit'].mean() / wining_trades['buy_price'].mean()) *100
				result = {'wining_trades':num_winings,
						  'buy_days'     :-buy_days,
						  'sell_days'    : sell_days,
						  'mean_profit'  : mean_profit}
				results.append(result)

	df = pd.DataFrame(results)
	if(len(df) > 0):
		print(df[['buy_days', 'sell_days', 'wining_trades', 'mean_profit']].sort_values(by=['mean_profit']))

	return meet

def find_stocks_meet_goal2(buy_day_range, sell_day_range, goal):	
	files = glob.glob('data/*.trades.csv')
	print("{} numer of stocks".format(len(files)))
	total_df = pd.DataFrame([])	
	for file in files:
		print(file)
		df = find_best_algo_params2(file, buy_day_range, sell_day_range, goal)
		
		if(len(df) > 0):
			if (len(total_df) == 0):
				total_df = df
			else:
				total_df = total_df.append(df)

	print(total_df)
	total_df.to_csv("data/good.stocks.csv")	
	print("{} good stocks".format(len(total_df.drop_duplicates(subset='stock', keep='last'))))



def find_best_algo_params2(trades, buy_day_range, sell_day_range, goal):
#	test_trades = pd.read_csv('data/' + ticker + '.trades.csv')
	meet = False
	test_trades = trades[['buy_date','buy_days','buy_price','month','profit','sell_date','sell_days','sell_price']]
	#test_trades = raw_test_trades.set_index('earning_date')

	results = pd.DataFrame([]);

	for sell_days in range(sell_day_range):
		for buy_days in range(buy_day_range):

			trades = test_trades[(test_trades.sell_days==sell_days) & (test_trades.buy_days==-buy_days)]
			
			wining_trades = trades[trades.profit>0]
			num_winings = len(wining_trades)
			
			if(num_winings > goal):
				if(len(results) == 0):
					results = trades
				else:
					results.append(trades)

	#another = results.assign(stock=trades_file)	
	print(results)
	return results


def check_operations(opt):
	supported_opts = 'dt'
	for c in opt[1:]:
		if c not in supported_opts:
			print_command_line_exit()

def group_summery(group):
	wining_trades = group[group.profit > 0]
	wining_trades['plp'] = wining_trades['profit'] / wining_trades['buy_price'] * 100
	lossing_trades = group[group.profit < 0]
	tied_trades = group[group.profit == 0]


	return pd.DataFrame({
		'num_win' : [len(wining_trades)],
		'mean_profit' : wining_trades['plp'].mean(),
		'min_profit'  : wining_trades['plp'].min(),
		'max_profit'  : wining_trades['plp'].max(),
		'num_loss': [len(lossing_trades)],
		'num_tie' : [len(tied_trades)],
		})

def summery(trades):
	cases = trades.groupby(['buy_days', 'sell_days'])
	summery = cases.apply(group_summery)
	return summery.sort_values(['num_win'], ascending=False)


def test_earning_on_date(date):
	tickers = earning_schedule(date)
	if(len(tickers) == 0):
		print ("no company announce earning on {}".format(date))
	else:
		testall(tickers, True)



def test_sp500(create_trades=False):
	skipped = []
	min_history = 6
	company_list = sp500()
	tickers = company_list.index.tolist()
	print(tickers)

	for ticker in tickers:
		print("Test {}".format(ticker))
		prices = price(ticker)
		earnings = earning(ticker)

		if((prices is not None) and (earnings is not None)):
			if(create_trades):
				create_test_trades(ticker)

		else:
			print("skip {} due to no enought history data".format(ticker))
			skipped.append(ticker)

	print("Following tickers skipped. {}".format(skipped))
			


def calculate_buy_sell_date(row):
	if(row['next_earnings'] == 'n/a'):
		row['buy_date'] = 'n/a'
		row['sell_date'] = 'n/a'
	else:
		next_earnings_date = datetime.strptime(row['next_earnings'], '%Y%m%d')
		row['buy_date'] =  (next_earnings_date + timedelta(days=row['buy_days'])).strftime('%Y%m%d')
		row['sell_date'] = (next_earnings_date + timedelta(days=row['sell_days'])).strftime('%Y%m%d')

	return row

def find_securities_for_trading_on_earning(date, days, generate_trades=False):
	start = datetime.strptime(date, '%Y-%m-%d')
	count = 0
	securities = pd.DataFrame()

	while count < days:
		day = start + timedelta(days=count)
		print(day)
		tickers_with_all_win_months = find_tickers_with_all_win_given_month(day.strftime('%Y-%m-%d'))
		securities = securities.append(tickers_with_all_win_months)		
		count = count+1

	securities['holding'] = securities.apply(lambda row : row['sell_days'] - row['buy_days'], axis=1)
	securities = securities[securities['num_win'] > 5]

	#merge current stock price
	prices = get_last_price(securities.index.tolist())
	prices = prices.loc[:,['close']]
	print(prices)

	securities = securities.merge(prices, how='inner', left_index=True, right_index=True)

	print(securities)
	securities.to_csv('data/signals.csv')

	securities.sort_values(['holding'], inplace=True)
	securities = securities[~securities.index.duplicated(keep='first')]
	print(securities)

	return securities;

def get_last_price(tickers):
	tickers = set(tickers)

	list = pd.DataFrame()

	for ticker in tickers:
		ph = price(ticker)

		if(len(ph) > 0):
			ph['ticker'] = ticker
			list = list.append(ph.head(1))

	return list.set_index('ticker')

def find_tickers_with_all_win_given_month(date, generate_trades=False):
	earning_schedule = download_earning_schedule(date)
	if(len(earning_schedule) == 0):
		return pd.DataFrame()

	earning_schedule['date'] = earning_schedule['date'].apply(lambda x : datetime.strptime(x, '%m/%d/%Y'))
	print(earning_schedule)

	tickers = find_tickers_with_all_win_months(earning_schedule['ticker'].tolist(), generate_trades);
	if(len(tickers) == 0):
		return pd.DataFrame()

	tickers = tickers.reset_index()
	tickers = tickers.set_index('ticker')
	print(tickers)
	
	earning_dates = earning_schedule.loc[:, ['date', 'month', 'ticker']]
	earning_dates.set_index('ticker', inplace=True,)
	print(earning_dates)

	tickers=tickers.merge(earning_dates, how='inner', left_index=True, right_index=True)
	print(tickers)
	tickers['buy_date'] = tickers.apply(lambda row : trading_date(row['date'], row['buy_days'], after_weekend=False), axis=1)
	tickers['sell_date'] = tickers.apply(lambda row : trading_date(row['date'], row['sell_days'], after_weekend=True), axis=1)

	print(tickers)

	tickers = tickers[tickers[['month_x', 'month_y']].apply(lambda row : row['month_x'] == row['month_y'][0:3], axis=1)]
	print(tickers)
	return tickers

def find_tickers_with_all_win_months(ticker_list, earning_schedule):	
	tickers_with_all_win_months = pd.DataFrame()
	for ticker in ticker_list:
		print("Find all win month for {}".format(ticker))
		trade_file = trade_file_name(ticker)
		if (isfile(trade_file)):
			trades = pd.read_csv(trade_file)
			groups = trades.groupby(['month', 'buy_days', 'sell_days'])
			summery = groups.apply(group_summery)
			all_win_months = summery[(summery.num_loss == 0) & (summery.num_tie == 0)].sort_values(by=['mean_profit'], ascending=False)
			if(len(all_win_months) > 0):
				all_win_months['ticker'] = ticker
				tickers_with_all_win_months = tickers_with_all_win_months.append(all_win_months)

	
	return tickers_with_all_win_months

def create_test_trades(ticker, buy_days_range=15, sell_days_range=15):
	print("Generating test trades for " + ticker)
	ph = price(ticker)
	eh = earning(ticker)
	if((ph is None) or (eh is None)):
		print("No history data for creating test trades")
		return None

	#Remove future earning date from earning history
	eh.reset_index(inplace=True)
	eh = eh[eh['date'].apply(lambda x : x < TODAY)]
	eh.set_index(['date'], inplace=True)

	print("\tPrice " + history_df_header(ph))
	print("\tEarning " + history_df_header(eh))

	if ((len(ph) <= 0) or (len(eh) <= 0)):
		print("No enough history data for creating test trades")
		return None

	list_trades = []
	for sell_days in range(sell_days_range):
		for buy_days in range(buy_days_range):
			trades = trade_on_earning(ph, eh, -buy_days, sell_days)

			list_trades.extend(trades)

	trades = pd.DataFrame(list_trades)
	trades = trades[['earning_date','month', 'buy_days', 'sell_days', 'buy_date', 'sell_date', 'buy_price','sell_price', 'profit', 'profit2']].set_index('earning_date').sort_values(['buy_days', 'sell_days'])
	trades.to_csv(trade_file_name(ticker))
	print("\t{} trades generated".format(len(trades)))
	return trades

def trade_on_earning(ph, eh, buy_before_earning_days, sell_after_earning_days):
	list_trades = []
	ph_start_date = ph.tail(1).iloc[0].name
	ph_end_date = ph.head(1).iloc[0].name

	for index, row in eh.iterrows():
		earning_date = index
		month = row['period'][:3]

		d = datetime.strptime(earning_date, '%Y-%m-%d')
		sell_date = date_plus(earning_date, sell_after_earning_days)
		buy_date  = date_plus(earning_date, buy_before_earning_days)

		## if we don't have enough price date for this earning date, what do we need to do?
		if ((buy_date >= ph_start_date) & (buy_date < ph_end_date) & (sell_date > ph_start_date) & (sell_date <= ph_end_date)):
			prices = ph.loc[sell_date:buy_date, ['close']]			
			if (len(prices) > 0):
				real_buy = prices.tail(1).iloc[0]
				real_sell = prices.head(1).iloc[0]

				sell_price = real_sell['close']
				buy_price  = real_buy['close']
				profit     = sell_price - buy_price
				profit2    = (profit / buy_price) * 100
				list_trades.append({'earning_date' : earning_date,
									'month'        : month,
									'buy_date'     : real_buy.name,
						 			'buy_price'    : buy_price,
					 				'sell_date'    : real_sell.name,
					 				'sell_price'   : sell_price,
					 				'profit'       : profit,
					 				'profit2'	   : profit2,
					 				'buy_days'     : buy_before_earning_days,
					 				'sell_days'    : sell_after_earning_days})

	return list_trades

def date_plus(sdate, days, fmt='%Y-%m-%d'):
	d = datetime.strptime(sdate, fmt) + timedelta(days=days)
	return d.strftime(fmt)

def trading_date(date, days, after_weekend=True):
	d = date + timedelta(days)
	if(after_weekend):
		move = 1
	else:
		move = -1

	while (d.weekday() > 4):
		d = d + timedelta(move)

	return d


pd.options.display.width = 1000









