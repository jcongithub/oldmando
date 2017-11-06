from stock import *
from datetime import datetime

class Strategy:

	def backtest(self, stock):
		earning_his = stock.earnings()
		Stock.show(earning_his, index=['date'])
		for earning in earning_his:
			print(earning)
			date = datetime.strptime(earning['date'], "%Y-%m-%d")
			print(date)
			days_before_earning = 5
			days_after_earning = 5
			prices = stock.range(date, days_before_earning, days_after_earning)
			Stock.show(prices, index=['date'])
			prices_before_earning = prices[:days_before_earning]
			prices_after_earning = prices[-days_after_earning:]

			Stock.show(prices_before_earning, index=['date'])
			Stock.show(prices_after_earning, index=['date'])

			trades = [];
			for bdays, bprice in enumerate(prices_before_earning):
				for sdays, sprice in enumerate(prices_after_earning):
					trade = { 
						'date_buy'  : bprice['date'],
						'date_sell' : sprice['date'],
						'price_buy' : bprice['close'],
						'price_sell': sprice['close'],
						'profit'    : sprice['close'] - bprice['close'],
						'buy_days'  : bdays,
						'sell_days' : sdays
					}

					trades.append(trade);

			Stock.show(trades, index=['date_buy'])


			