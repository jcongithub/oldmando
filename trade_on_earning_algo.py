import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

from datetime import datetime
from datetime import timedelta
#from printer import print_dataframe_as_table
import glob
from downloader import download_price_history
from downloader import download_earning_history
from downloader import download_history
from downloader import download_earning_schedule
from downloader import download_sp500_stoct_list

stock = {}

is_debug = False
def debug(msg):
	if(is_debug):
		print(msg)

def info(msg):
	print(msg)


def date_plus(sdate, days, fmt='%Y-%m-%d'):
	d = datetime.strptime(sdate, fmt) + timedelta(days=days)
	return d.strftime(fmt)


def algo_trade_on_earning(ph, eh, buy_before_earning_days, sell_after_earning_days):
	list_trades = []
	ph_start_date = ph.tail(1).iloc[0].name
	ph_end_date = ph.head(1).iloc[0].name

	for index, row in eh.iterrows():
		earning_date = index
		month = row['Period Ending'][:3]

		d = datetime.strptime(earning_date, '%Y-%m-%d')
		sell_date = date_plus(earning_date, sell_after_earning_days)
		buy_date  = date_plus(earning_date, buy_before_earning_days)

		debug('earning_date:{} buy_date:{} sell_date:{}'.format(earning_date, buy_date, sell_date))

		## if we don't have enough price date for this earning date, what do we need to do?
		if ((buy_date >= ph_start_date) & (buy_date < ph_end_date) & (sell_date > ph_start_date) & (sell_date <= ph_end_date)):
			prices = ph.loc[sell_date:buy_date, ['Close']]			
			if (len(prices) > 0):
				real_buy = prices.tail(1).iloc[0]
				real_sell = prices.head(1).iloc[0]

				sell_price = real_sell['Close']
				buy_price  = real_buy['Close']
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

def trade_on_earning(ticker, buy_days_range=15, sell_days_range=15):
	ph = pd.read_csv('data/' + ticker + '.price.csv')
	eh = pd.read_csv('data/' + ticker + '.earning.csv')

	ph = ph.set_index('Date')
	eh = eh.set_index('Date')
	
	info('{} days price history from {} to {}'.format(len(ph), ph.iloc[len(ph) - 1].name, ph.iloc[0].name))
	info("{} earning history from {} to {}".format(len(eh), eh.iloc[len(eh) -1].name, eh.iloc[0].name))
	info('Test trading on buying between {} days before earning day and selling between 0 - {} days'.format(buy_days_range, sell_days_range))

	list_trades = []
	for sell_days in range(sell_days_range):
		for buy_days in range(buy_days_range):
			trades = algo_trade_on_earning(ph, eh, -buy_days, sell_days)

			list_trades.extend(trades)

	trades = pd.DataFrame(list_trades)
	trades = trades[['earning_date','month', 'buy_days', 'sell_days', 'buy_date', 'sell_date', 'buy_price','sell_price', 'profit', 'profit2']].set_index('earning_date').sort_values(['buy_days', 'sell_days'])
	return trades

def performance(trades):
	df = pd.DataFrame(trades)
#	print(df[['earning_date', 'buy_date', 'sell_date', 'buy_price', 'sell_price', 'profit']])		

	num_wining = df[df.profit > 0]['earning_date'].count()
	num_losing = df[df.profit < 0]['earning_date'].count()
	num_tie	   = df[df.profit == 0]['earning_date'].count()
	profit     = df['profit'].sum()

	summery = {'wining' : num_wining,
			   'losing' : num_losing,
			   'tie'    : num_tie,
			   'profit' : profit}

	return summery

def show_result(algo_return_df):
	df_winlose = algo_return_df['wining'] - algo_return_df['losing']
	x = algo_return_df['buy_days'].tolist()
	y = algo_return_df['sell_days'].tolist()
	area = 30
	colors = []
	for wl in df_winlose:
		if wl > 10:
			colors.append('green')
		elif wl > 0 & wl <10:
			colors.append('yellow')
		else:
			colors.append('red')


	plt.scatter(x, y, s=area, c=colors)
	plt.show()


def anlaysis(ticker):
	ph = pd.read_csv('data/' + ticker + '.price.csv')
	eh = pd.read_csv('data/' + ticker + '.earning.csv')

	ph = ph.set_index('Date')
	eh = eh.set_index('Date')
	
	debug('{} days price history from {} to {}'.format(len(ph), ph.iloc[len(ph) - 1].name, ph.iloc[0].name))
	debug("{} earning history from {} to {}".format(len(eh), eh.iloc[len(eh) -1].name, eh.iloc[0].name))

	algo_return_comparison = []

	for sell_days in range(15):
		for buy_days in range(15):
			trades = algo_trade_on_earning(ph, eh, -buy_days, sell_days)
			summery = performance(trades)
			summery['buy_days'] = -buy_days
			summery['sell_days'] = sell_days
			algo_return_comparison.append(summery)

	algo_return_df = pd.DataFrame(algo_return_comparison)
	algo_return_df.to_csv('data/' + ticker + '.algo.csv')

def play(ticker):
	df = pd.read_csv('data/' + ticker + '.algo.csv')
	print(df)
	print(df[df.wining > 20])

def run_all(buy_days, sell_days):
	df = pd.read_csv('sp500.csv')
	print(df['Ticker'])
	for ticker in df['Ticker']:
		print("Run algo on {} ".format(ticker), end="")
		try:
			test_algo_trade_on_earning(ticker, buy_days, sell_days)
			print("Ok")
		except:

			print("Failed")

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


def print_command_line_exit():
	print("trade_on_earning_algo [-d] tickers")
	exit()

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

def tickers():
	tickers = ['aeo', 'msn', 'csco']
	print(tickers)

def summery(trades):
	cases = trades.groupby(['buy_days', 'sell_days'])
	summery = cases.apply(group_summery)
	#print(summery.sort_values(['num_win'], ascending=False).head(10))
	return summery.sort_values(['num_win'], ascending=False)


def test_earning_on_date(date):
	tickers = earning_schedule(date)
	if(len(tickers) == 0):
		print ("no company announce earning on {}".format(date))
	else:
		testall(tickers, True)

def testall(tickers, refresh=False):
	bests = pd.DataFrame()

	for ticker in tickers:
		print("Back test {}".format(ticker))
		if(refresh):
			download_history(ticker)

		trades = None
		if(refresh):
			try:	
				trades = trade_on_earning(ticker)
			except:
				print("Failed to do back test for {}".format(ticker))
				print("Unexpected error:", sys.exc_info()[0])
		else:
			trades = load(ticker)

		if(trades is not None):
			file_path = 'data/' + ticker + '.trades.csv'
			trades.to_csv(file_path)
			cases = summery(trades)
			cases['ticker'] = ticker
			best = cases.head(1)
			bests = bests.append(best)

	print(bests)

def test_snp500():
	company_list = download_sp500_stoct_list()
	tickers = []

	for company in company_list:
		ticker = company['ticker']
		tickers.append(ticker)

	print("Total {} tickers".format(len(tickers)))

	testall(tickers, True)


def load(ticker):
	trades = pd.read_csv('data/' + ticker + ".trades.csv")
	prices = pd.read_csv('data/' + ticker + ".price.csv")
	earnings = pd.read_csv('data/' + ticker + ".earning.csv")

	print("{} {} trades {} prices {} earnings".format(ticker, len(trades), len(prices), len(earnings)))
	global stock
	stock = {'ticker' : ticker,
			 'trades' : trades,
			 'prices' : prices,
			 'earnings': earnings
			}
	return trades	

def earning_schedule(date):
	tickers = download_earning_schedule(date)
	return [x.lower() for x in tickers]


def thelp():
	command_summery_format = "{:<35}{}"
	print(command_summery_format.format('testall(tickers, refresh=False)', 'find best buy sell days for given securities'))
	print(command_summery_format.format('test_earning_on_date(date)', 'find best buy sell days for all securities which will announce the earning on given date'))
	print(command_summery_format.format('earning_schedule(date)', 'returns a list of securities will announce the earnign on given date'))
	print('\t date:2016-Dec-06')

def find_best_month(ticker):
	trades = pd.read_csv('data/' + ticker + '.trades.csv')
	groups = trades.groupby(['month', 'buy_days', 'sell_days'])
	summery = groups.apply(group_summery)
	#print(summery.sort_values(by=['num_win'], ascending=False).head(5))
	print(summery[(summery.num_loss == 0) & (summery.num_tie == 0)].sort_values(by=['mean_profit'], ascending=False))			

pd.options.display.width = 1000
#
#print(trades)
#
#
#print(summery)
#print()
