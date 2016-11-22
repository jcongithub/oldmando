import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

from datetime import datetime
from datetime import timedelta
#from printer import print_dataframe_as_table
import glob
from download import download_price_history
from download import download_earning_history

is_debug = True
def debug(msg):
	if(is_debug):
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

				list_trades.append({'earning_date' : earning_date,
									'month'        : month,
									'buy_date'     : real_buy.name,
						 			'buy_price'    : buy_price,
					 				'sell_date'    : real_sell.name,
					 				'sell_price'   : sell_price,
					 				'profit'       : profit,
					 				'buy_days'     : buy_before_earning_days,
					 				'sell_days'    : sell_after_earning_days})

	return list_trades

def test_algo_trade_on_earning(ticker, buy_days, sell_days):
	ph = pd.read_csv('data/' + ticker + '.price.csv')
	eh = pd.read_csv('data/' + ticker + '.earning.csv')

	ph = ph.set_index('Date')
	eh = eh.set_index('Date')
	
	debug('{} days price history from {} to {}'.format(len(ph), ph.iloc[len(ph) - 1].name, ph.iloc[0].name))
	debug("{} earning history from {} to {}".format(len(eh), eh.iloc[len(eh) -1].name, eh.iloc[0].name))

	list_trades = []
	for sell_days in range(sell_days):
		for buy_days in range(buy_days):
			trades = algo_trade_on_earning(ph, eh, -buy_days, sell_days)
			list_trades.extend(trades)

	df = pd.DataFrame(list_trades)
	df.to_csv('data/' + ticker + ".trades.csv")
	return df.set_index('earning_date')


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



def find_best_algo_params2(trades_file, buy_day_range, sell_day_range, goal):
#	test_trades = pd.read_csv('data/' + ticker + '.trades.csv')
	meet = False
	raw_test_trades = pd.read_csv(trades_file)
	raw_test_trades = raw_test_trades[['earning_date','buy_date','buy_days','buy_price','month','profit','sell_date','sell_days','sell_price']]
	test_trades = raw_test_trades.set_index('earning_date')
	test_trades.to_csv(trades_file)

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

	another = results.assign(stock=trades_file)	
	print(another)
	return another


if __name__ == '__main__':
	pd.options.display.width = 1000
	BUY_DAYS = 15
	SELL_DAYS = 15
	GOAL = 22

	if(len(sys.argv) <= 1):
		print("trade_on_earning_algo ticker")
	else:
		ticker = sys.argv[1]
		download_price_history(ticker)
		download_earning_history(ticker)
		trades = test_algo_trade_on_earning(ticker, BUY_DAYS, SELL_DAYS)

		print(trades)


#	find_stocks_meet_goal2(BUY_DAYS, SELL_DAYS, GOAL)
	#find_best_algo_params2('data/AJG.trades.csv', BUY_DAYS, SELL_DAYS, GOAL)


#	run_all(BUY_DAYS, SELL_DAYS)
#	


	#df = pd.read_csv("data/MSFT.earning.csv")
	#df['Month'] = df.apply(lambda row: row['Period Ending'][:3] ,axis=1)
	#print(df)
	#print(df.groupby(['Month']).agg('count'))

	