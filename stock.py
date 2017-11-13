from dao import conn
from datetime import datetime
import pandas as pd

class Stock:
	def __init__(self, ticker):
		self.ticker = ticker

	def __str__(self):
		return self.ticker

	def earnings(self):
		sql = "select * from earnings where ticker = :ticker order by date";
		row_convertor = lambda row: {
				#'ticker' 	: row[0],
				'date'		: row[1],
				'estimate'	: row[2],
				'period'	: row[3],
				'reported'	: row[4],
				'surprise1'	: row[5],
				'surprise2'	: row[6]
			}
		
		result = self.run_query(sql, {'ticker':self.ticker}, row_convertor)
		#return pd.DataFrame.from_records(result, index='date')
		return result

	def before(self, date):
		sdate = date.strftime('%Y%m%d')
		sql = "select * from prices where ticker='" + self.ticker + "' and date <='" + sdate + "' order by date desc limit 1"
		return self.get_price(sql)


	def after(self, date):
		sdate = date.strftime('%Y%m%d')
		sql = "select * from prices where ticker='" + self.ticker + "' and date >='" + sdate + "' order by date limit 1"
		return self.get_price(sql)

	def range(self, date, days_before, days_after):
		sdate = date.strftime('%Y-%m-%d')
		sql = "select * from (select * from prices where ticker=:ticker and date < :date order by date desc limit :days_before) union all select * from (select * from prices where ticker=:ticker and date >:date order by date limit :days_after) union all select * from (select * from prices where ticker=:ticker and date =:date order by date limit :days_after) order by date"
		return self.get_price(sql, {'ticker':self.ticker, 'date': sdate, 'days_before':days_before, 'days_after' : days_after + 1})


	def get_price(self, sql, params={}):
		#print(sql)
		cur = conn.execute(sql, params)
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

	def run_query(self, sql, params={}, row_convertor={}):
		#print(sql)
		cur = conn.execute(sql, params)
		rows = cur.fetchall()
		cur.close()

		return list(map(row_convertor, rows))

	def list():
		sql = 'select * from stocks order by ticker'
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

	def query(sql):
		return pd.read_sql(sql, conn);

	def show(list_dict, index, columns=None):
		print(pd.DataFrame.from_records(list_dict, index=index, columns=columns))

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

	def backtest(ticker):
		stock = Stock(ticker)
		earnings = stock.earnings()
		Stock.show(earnings, 'date')
		for e in earnings :
			report_date = datetime.strptime(e['date'], '%Y-%m-%d')
			print('{} {}'.format(e['date'], e['surprise1']))
			prices = stock.range(report_date, 1, 1)
			Stock.show(prices, 'date')
			print('')


#if __name__ == '__main__':

