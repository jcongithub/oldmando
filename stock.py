from dao import conn
from datetime import datetime

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

if __name__ == '__main__':

	adm = Stock('TSLA')

	print(str(adm))
	print(str(adm.after(datetime(2017, 7, 31))))
	print(str(adm.after(datetime(2017, 8, 5))))
