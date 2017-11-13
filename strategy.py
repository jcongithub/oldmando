from stock import *
from datetime import datetime

class Strategy:
	def backtestAll():
		stocks = Stock.list()

		for stock in stocks:
			ticker = stock['ticker']
			print ('Generating test trades of stock:{}'.format(ticker))
			trades = Strategy.backtest(Stock(ticker))
			Strategy.save_test_trades(ticker, trades)


	def backtest(stock):
		days_before_earning = 10
		days_after_earning = 10
		earning_his = stock.earnings()
		
		trades = [];		
		for earning in earning_his:
			date = datetime.strptime(earning['date'], "%Y-%m-%d")
			prices = stock.range(date, days_before_earning, days_after_earning)
#			Stock.show(prices, index=['date'])

			if(len(prices) >= days_before_earning + days_after_earning):
				prices_before_earning = prices[:days_before_earning]
				prices_after_earning = prices[-days_after_earning:]

				for bdays, bprice in enumerate(prices_before_earning):
					for sdays, sprice in enumerate(prices_after_earning):
						trade = { 
							'ticker'    : stock.ticker,
							'date_earning': earning['date'],
							'date_buy'  : bprice['date'],
							'date_sell' : sprice['date'],
							'buy_price' : bprice['close'],
							'sell_price': sprice['close'],
							'profit1'   : sprice['close'] - bprice['close'],
							'profit2'	: (sprice['close'] - bprice['close'])*100/bprice['close'], 
							'buy_days'  : bdays - len(prices_before_earning),
							'sell_days' : sdays + 1,
							'holding_days' : sdays + 1 - bdays + len(prices_before_earning)
						}

						trades.append(trade);
		return trades

	def save_test_trades(ticker, trades):
		conn.execute("delete from trades where ticker=:ticker", {'ticker' : ticker})

		for index, trade in enumerate(trades):
			conn.execute("insert into trades values(:ticker, :date_earning, :date_buy, :date_sell, :buy_price, :sell_price, :profit1, :profit2, :buy_days, :sell_days, :holding_days)", trade)

		conn.commit();

		




			