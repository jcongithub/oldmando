import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

from datetime import datetime
from datetime import timedelta
from os import listdir
from os.path import isfile, join
import glob
import os
import csv
import string

import mpf
import dao
import downloader

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
		'num_win'     : [len(wining_trades)],
		'mean_profit' : wining_trades['plp'].mean(),
		'min_profit'  : wining_trades['plp'].min(),
		'max_profit'  : wining_trades['plp'].max(),
		'num_loss'    : [len(lossing_trades)],
		'num_tie'     : [len(tied_trades)],
		})

def test_earning_on_date(date):
	tickers = earning_schedule(date)
	if(len(tickers) == 0):
		print ("no company announce earning on {}".format(date))
	else:
		testall(tickers, True)



			


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
	schedules = earning_schedule(date)
	if(len(schedules) == 0):
		print("No earning reort on {}".format(date))
		return pd.DateFrame()

	schedules['date'] = schedules['date'].apply(lambda x : datetime.strptime(x, '%m/%d/%Y'))
	print(schedules)

	tickers = find_all_win_months(schedules['ticker'].tolist());
	if(len(tickers) == 0):
		return pd.DataFrame()

	tickers = tickers.reset_index()
	tickers = tickers.set_index('ticker')
	print(tickers)
	
	earning_dates = schedules.loc[:, ['date', 'month', 'ticker']]
	earning_dates.set_index('ticker', inplace=True,)
	print(earning_dates)

	tickers=tickers.merge(earning_dates, how='inner', left_index=True, right_index=True)
	print(tickers)

	tickers = tickers[tickers.apply(lambda row : row['month_y'][:3] == row['month_x'], axis=1)]
	print(tickers)


	tickers['buy_date'] = tickers.apply(lambda row : trading_date(row['date'], row['buy_days'], after_weekend=False), axis=1)
	tickers['sell_date'] = tickers.apply(lambda row : trading_date(row['date'], row['sell_days'], after_weekend=True), axis=1)

	print(tickers)
	return tickers

def find_all_win_months(ticker_list):	
	ticker_month = pd.DataFrame()
	for ticker in ticker_list:
		print("Find all win month for {}".format(ticker))
		trade_file = trade_file_name(ticker)
		if (isfile(trade_file)):
			trades = pd.read_csv(trade_file)
			groups = trades.groupby(['month', 'buy_days', 'sell_days'])
			summery = groups.apply(group_summery)

			months = summery[(summery.num_loss == 0) & (summery.num_tie == 0)]
			
			if(len(months) > 0):
				months.reset_index(inplace=True)
				months['holding'] = months.apply(lambda x : x['sell_days'] - x['buy_days'], axis=1)
				months.sort_values('holding', inplace=True)
				months = months[~months.duplicated(['month'], keep='first')]
				months['ticker'] = ticker
				months.drop(['num_loss', 'num_tie', 'level_3'], axis=1, inplace=True)
				ticker_month = ticker_month.append(months)

	
	return ticker_month

def find_winning_month_s(tickers, start_year, years):
	if tickers is None:
		tickers = [f for f in listdir('data/') if 'trade.csv' in f]
		tickers = [f[:-10] for f in tickers]

	print(tickers)
	print("{} tickers".format(len(tickers)))

	result = pd.DataFrame()
	for ticker in tickers:
		months = find_winning_month(ticker, start_year, years)
		if(months is not None and len(months) > 0):
			result = result.append(months.reset_index(drop=True).set_index(['month']))
	return result

def find_winning_month(ticker, start_year, years):
	print(ticker)
	earning_history = earning(ticker, start_year, years)
	trade_list = testtrade(ticker, earning_history.index.tolist())

	months = None
	if(len(trade_list) > 0):
		groups = trade_list.groupby(['month', 'buy_days', 'sell_days'])
		summery = groups.apply(group_summery)

		months = summery[(summery.num_loss == 0) & (summery.num_tie == 0)]
					
		if(len(months) > 0):
			months.reset_index(inplace=True)
			months['holding'] = months.apply(lambda x : x['sell_days'] - x['buy_days'], axis=1)
			months.sort_values('holding', inplace=True)
			months = months[~months.duplicated(['month'], keep='first')]
			months['ticker'] = ticker
			months.drop(['num_loss', 'num_tie', 'level_3'], axis=1, inplace=True)

	return months


def history_df_header(e):
	if(len(e) > 0):
		return 'records:{} from:{} to:{}'.format(len(e), e.tail(1).index[0], e.head(1).index[0])
	else:
		return 'records:0'

def create_test_trades(tickers, buy_days_range=15, sell_days_range=15):
	skipped = []
	
	for ticker in tickers:
		mpf.task_start("GenerateTestTrade")
		
		print("Generating test trades for " + ticker)

		mpf.task_start("ReadData")		
		ph = dao.price(ticker)
		eh = dao.earning(ticker)
		mpf.task_end("ReadData")

		if((ph is None) or (eh is None) or ph.empty or eh.empty):
			print("No history data for {}".format(ticker))
			skipped.append(ticker)
		else:
			mpf.task_start("Calculate")
			print("\tPrice " + history_df_header(ph))
			print("\tEarning " + history_df_header(eh))

			list_trades = []
			for sell_days in range(sell_days_range):
				for buy_days in range(buy_days_range):
					trades = trade_on_earning2(ticker, eh, -buy_days, sell_days)

					list_trades.extend(trades)

			trades = pd.DataFrame(list_trades)
				
			if(len(trades) > 0):				
				trades = trades[['earning_date','period', 'buy_days', 'sell_days', 'buy_date', 'sell_date', 'buy_price','sell_price', 'profit', 'profit2']].set_index('earning_date').sort_values(['buy_days', 'sell_days'])
				dao.save_test_trades(ticker, trades)
				print("\t{} trades generated".format(len(trades)))
			
			mpf.task_end("Calculate")

		mpf.task_end("GenerateTestTrade")

	if(len(skipped) > 0):
		print("Following tickers were skipped due to no enough history data")
		print(skipped)

def trade_on_earning2(ticker, eh, buy_before_earning_days, sell_after_earning_days):
	mpf.task_start("OneTradeForAllEarning")
	list_trades = []

	for index, row in eh.iterrows():

		mpf.task_start("OneTestTrade")
		earning_date = index
		period = row['period']

		mpf.task_start("CalculateBuySellDate")
		sell_date = date_plus(earning_date, sell_after_earning_days)
		buy_date  = date_plus(earning_date, buy_before_earning_days)
		mpf.task_end("CalculateBuySellDate")
		
		mpf.task_start("FindBuySellPrices")
		real_buy  = dao.price1(ticker, buy_date)
		real_sell = dao.price1(ticker, sell_date)
		mpf.task_end("FindBuySellPrices")

		if(len(real_buy) > 0 and len(real_sell) > 0):
			mpf.task_start("CalculateRealBuySellPrice")
			sell_price = real_sell[0][4]
			buy_price  = real_buy[0][4]
			profit     = sell_price - buy_price
			profit2    = (profit / buy_price) * 100
			mpf.task_end("CalculateRealBuySellPrice")
					
			mpf.task_start("AppendTrade")
			list_trades.append({'earning_date' : earning_date,
								'period'       : period,
								'buy_date'     : buy_date,
					 			'buy_price'    : buy_price,
				 				'sell_date'    : sell_date,
				 				'sell_price'   : sell_price,
				 				'profit'       : profit,
				 				'profit2'	   : profit2,
				 				'buy_days'     : buy_before_earning_days,
				 				'sell_days'    : sell_after_earning_days})
			mpf.task_end("AppendTrade")

		mpf.task_end("OneTestTrade")
			
	mpf.task_end("OneTradeForAllEarning")

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

def trade_schedule(quarter, min_win=5):
	s = pd.read_csv('data/signals.csv')
	s = s[(s['month'] == quarter) & (s['num_win'] > min_win)]
	s.set_index(['ticker'], inplace=True)

	#build earning schedule and current price
	eps = pd.DataFrame()
	for ticker in s.index.tolist():
		e = earning(ticker)
		p = price(ticker)

		ep = {'ticker'  : ticker,
			   'edate'  : e.index[0],
			   'quarter': e.iloc[0]['quarter'],
			   'price'  : p.iloc[0]['close']}

		eps = eps.append(ep, ignore_index=True)
	eps.reset_index(inplace=True)
	eps.set_index(['ticker'], inplace=True)

	#concat the current price and earning schedule
	s = pd.concat([s, eps], axis=1)
	
	s['edate'] = s['edate'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d'))
	s['buy_date'] = s.apply(lambda row : trading_date(row['edate'], row['buy_days'], after_weekend=False), axis=1)
	s['sell_date'] = s.apply(lambda row : trading_date(row['edate'], row['sell_days'], after_weekend=True), axis=1)
	s.sort_values(['edate'], inplace=True)
	s = s.loc[:, ['edate', 'buy_date', 'sell_date', 'price', 'mean_profit', 'max_profit', 'min_profit', 'num_win', 'holding']]
	s = s.rename(columns={'buy_date': 'buy@', 'sell_date':'sell@', 'mean_profit':'mean', 'max_profit':'max', 'min_profit':'min', 'num_win':'wins'})
	s['edate'] = s['edate'].apply(lambda x : datetime.strftime(x, '%m-%d'))
	s['buy@'] = s['buy@'].apply(lambda x : datetime.strftime(x, '%m-%d'))
	s['sell@'] = s['sell@'].apply(lambda x : datetime.strftime(x, '%m-%d'))
	s['pnl'] = s.apply(lambda x : (float(x['price'] * float(x['mean']))/100), axis=1)

	s.sort_values(['buy@'], inplace=True)

	daily_banlance(s)

	return s

def daily_banlance(trades):
	buys = trades.loc[:, ['buy@', 'price']].sort_values(['buy@'])
	sells = trades.loc[:, ['sell@', 'price', 'mean']].sort_values(['sell@'])
	sells['price'] = trades.apply(lambda row : (float(row['price']) + float(row['pnl'])), axis=1) 

	start = datetime.strptime(buys.iloc[0]['buy@'], '%m-%d')
	end   = datetime.strptime(sells.iloc[-1]['sell@'], '%m-%d')

	p = 0
	max_p = 0

	while (start <= end):
		date = datetime.strftime(start, '%m-%d')
		buy = buys[buys['buy@'] == date]
		sell = sells[sells['sell@'] == date]

		if(len(buy) > 0):
			p = p - buy.sum().price
		
		if(len(sell) > 0):
			p = p + sell.sum().price

		print("{0} {1:10.2f} Buy {2} Sell {3}".format(date, p, buy.index.tolist(), sell.index.tolist()))

		if (p < max_p):
			max_p = p

		start = start + timedelta(1)

	print("Pricipal required {} PnL {}".format(-max_p, trades['pnl'].sum()))

def csv2html(csv_file_name, html_file_name):
	table_string = ""
	s = pd.read_csv(csv_file_name)
	s.sort_values(['buy_date'], inplace=True, ascending=True)
	table_string = "<tr><td>ticker</td><td>buy_date</td><td>sell_date</td><td>date</td><td>holding</td><td>max_profit</td><td>min_profit</td><td>mean_profit</td></tr>"
	for index, row in s.iterrows():
		table_string += "<tr>"
		ticker = row['ticker']
		table_string += '<td>' + '<a href="https://www.google.com/finance?q=' + ticker + '">' + ticker + '</a></td>'
		table_string += '<td>' + row['buy_date'] + "</td>"
		table_string += '<td>' + row['sell_date'] + "</td>"
		table_string += '<td>' + row['date'] + "</td>"
		table_string += '<td>' + str(row['holding']) + "</td>"
		table_string += '<td>' + '{:.2f}'.format(row['max_profit']) + "</td>"
		table_string += '<td>' + '{:.2f}'.format(row['min_profit']) + "</td>"
		table_string += '<td>' + '{:.2f}'.format(row['mean_profit']) + "</td>"
		table_string += "</tr>"

	return '<html><head><link rel="stylesheet" type="text/css" href="a.css"></head><body><table>' + table_string + '</table></body></html>'    


def trading_signal(start_date = datetime.now(), number_days = 20):
	print("Find all back tested stocks scheduled for earning announcement in given dates...")
	earning_data_list = dao.schedule(start_date, number_days)
	print(earning_data_list)
	
	winning_stocks = []
	for earning_data in earning_data_list:
		ticker = earning_data['ticker']
		quarter = earning_data['quarter']
		print("ticker:{} quarter:{}".format(ticker, quarter))

		summary = dao.test_summary(ticker, quarter)
		if (len(summary['total_periods']) > 4) and (len(summary['winning_periods']) > 3):
			summary['ticker'] = ticker
			summary['quarter'] = quarter
			print(summary)
			winning_stocks.append(summary)

	return winning_stocks

def backtest(stock, start_days, end_days):
	for e in stock.earning_history():
		start_date = e.date()
		end_date = e.date()
		for start_day in range[0, start_days]:
			for end_day in range[0, end_days]:
				start_price = stock.price_before(start_date)
				end_price = stock.price_end(end_date)
				start_date = start_price.date()
				end_date = end_price.date()



#def weekly_calculation():
	#download earning schedule
	#start_date = datetime.now() + timedelta(days = 14)
	#num_days = 7
	#print("Downloading {} days earning schedule from {}".format(num_days, start_date.strftime('%Y-%m-%d')))
	#earning_schedule = downloader.download_earning_schedule(start_date, num_days)
	#print(pd.DataFrame(earning_schedule))
	#tickers = [s['ticker'] for s in earning_schedule]
	#print(tickers)

	#save earning schedule
	##dao.save_earning_schedule(earning_schedule);
	##print("Earning schedule saved")

	#download price history
	##print("Downloading price history for {}".format(tickers))
	##downloader.download_price_history(tickers)

	#download earning history
	##print("Downloading earning history for {}".format(tickers))
	##downloader.download_earning_history(tickers)

	#calculate back test trades
	#create_test_trades(tickers)
	
	#generate trade plan


pd.options.display.width = 1000

if __name__ == '__main__':
	sql = 	('SELECT *' 
			' FROM ('
  			'	SELECT a.ticker as ticker, a.date as date, a.quarter as quarter, b.period as period, b.buy_days as buy_days, b.sell_days as sell_days, b.profit2 as profit2'
  			'	FROM ('
    		'		SELECT a.ticker as ticker, a.date as date, substr(a.period, 0, 4) as quarter'
    		'		FROM s.schedule a, a.quarter_win b'
    		'		WHERE date > :date and a.ticker = b.ticker and substr(a.period, 0, 4) = b.quarter and b.total_periods_tested = b.win_periods_tested'
    		'		ORDER BY date'
  			'	) a, t.trades b'
  			'	WHERE a.ticker = b.ticker and a.quarter = substr(b.period, 0, 4) and b.profit > 0'
  			'	GROUP BY a.ticker, b.period'
  			'	HAVING (b.sell_days - b.buy_days) = min(b.sell_days - b.buy_days)'
			' )'
			' GROUP BY ticker '
			' HAVING profit2 = max(profit2)'
			' ORDER BY date')

	print("Calling SQL:{}".format(sql))
	
	df = pd.read_sql(sql, dao.conn, params = {'date' : '2017-04-12'})
	
	print(df)
	table = '<table>'
	table = table + '<tr><th>' + '</th><th>'.join(df.columns) + '</th></trades>'

	rows = df.values.tolist()

	for row in rows:
		table_row = '<tr><td>' + '</td><td>'.join([str(cell) for cell in row]) + '</td></tr>'
		table = table + table_row
	
	table = table + '</table>'
	html = '<html><head><link rel="stylesheet" type="text/css" href="a.css"></head>' + table + '</html>'

	print(html)