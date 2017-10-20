from dao import conn
from datetime import datetime
import pandas as pd

class Stock:
	def __init__(self, ticker):
		self.ticker = ticker

	def __str__(self):
		return self.ticker

	def before(self, date):
		sdate = date.strftime('%Y%m%d')
		sql = "select * from prices where ticker='" + self.ticker + "' and date <='" + sdate + "' order by date desc limit 1"
		return self.get_price(sql)

	def after(self, date):
		sdate = date.strftime('%Y%m%d')
		sql = "select * from prices where ticker='" + self.ticker + "' and date >='" + sdate + "' order by date limit 1"
		return self.get_price(sql)

	def get_price(self, sql):
		print(sql)
		cur = conn.execute(sql, {})
		rows = cur.fetchall()
		prices = [{'ticker'    : row[0], 
				   'date'      : row[1],
				   'open'      : row[2],
				   'high'      : row[3],
				   'low'       : row[4],
				   'close'     : row[5],
				   'volumn'    : row[6],
				   'adj_close' : row[7]} for row in rows]
		cur.close()
		return prices

	def list():
		sql = 'select * from stocks'
		cur = conn.execute(sql, {})
		rows = cur.fetchall()
		stocks = [{
			'ticker' 	: row[0],
			'name'   	: row[1],
			'industry'	: row[2],
			'sector'	: row[3],
			'size'		: row[4],
			'exchange'	: row[5]
		} for row in rows]

		return stocks

	def current_earning(tickers):
		ticker_list = str(tickers)[1:-1]
		print('ticker_list:{}'.format(ticker_list))
		sql = "select * from earnings where ticker in ({}) group by ticker having date=max(date)".format(ticker_list);
		print('sql:{}'.format(sql))
		cur = conn.execute(sql, {})
		rows = cur.fetchall()
		earnings = [{
			'ticker' 	: row[0],
			'date'		: row[1],
			'estimate'	: row[2],
			'period'	: row[3],
			'reported'	: row[4],
			'surprise1'	: row[5],
			'surprise2'	: row[6]
		} for row in rows]

		cur.close()
		return earnings;

	def earning_on(date):
		sql = "select * from earnings where date = '{}'".format(date)
		print(sql)
		cur = conn.execute(sql, {})
		rows = cur.fetchall()
		cur.close()

		earnings = [{
			'ticker' 	: row[0],
			'date'		: row[1],
			'estimate'	: row[2],
			'period'	: row[3],
			'reported'	: row[4],
			'surprise1'	: row[5],
			'surprise2'	: row[6]
		} for row in rows]

		return earnings


	def test():
		adm = Stock('TSLA')
		print(str(adm))
		print(str(adm.after(datetime(2017, 7, 31))))
		print(str(adm.after(datetime(2017, 8, 5))))

		print(pd.DataFrame(Stock.list()))
		df = pd.DataFrame(Stock.current_earning(['amd', 'tsla', 'kors', 'nflx']))
		print(df.set_index('ticker'))

		df = pd.DataFrame(Stock.earning_on('2017-10-16'))
		df = df.set_index(['ticker'])
		print(df)

#if __name__ == '__main__':

