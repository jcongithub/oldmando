import sqlite3
import pandas as pd
from os.path import isfile, join
import mpf
import msqlite
from datetime import datetime
from datetime import timedelta

SCHEDULE_TABLE = "s.schedule"
EARNING_HISTORY_TABLE = "earnings"
PRICE_HISTORY_TABLE   = "prices"
STOCK_TABLE           = "stocks"
TRADE_TABLE           = "t.trades"

pd.options.display.width = 1000

conn = sqlite3.connect('db/history')
cur = conn.cursor()
cur.execute("ATTACH DATABASE 'db/trades' AS t")
cur.execute("ATTACH DATABASE 'db/schedule' AS s")
cur.execute("ATTACH DATABASE 'db/analytics' AS a")

def all_stock_symbols():
	return pd.read_sql('select * from stocks', conn, index_col='ticker')

def price(ticker, date=None):
	sql = "select date,open,high,low,close,volume,adj_close from " + PRICE_HISTORY_TABLE + " where ticker='" + ticker + "' order by date desc"
	return pd.read_sql(sql, conn)

def earning(ticker):
	TODAY = datetime.now().strftime('%Y-%m-%d')
	cur = conn.execute("select date,estimate,period,reported,surprise1,surprise2 from " + EARNING_HISTORY_TABLE + " where ticker=:ticker and date < :date order by date desc", {'ticker' : ticker, 'date' : TODAY})
	earnings = pd.DataFrame(cur.fetchall(), columns=['date','estimate','period','reported','surprise1','surprise2'])
	return earnings.set_index(['date'])

def sp500():
	file_name = 'data/sp500.csv'
	if(not isfile(file_name)):
		download_sp500_company_list()

	return pd.read_csv(file_name, index_col=['ticker'])
	
##############################################################################################
def schedule(start_date=datetime.now(), number_days=15, tested_stocks_only=True):
	end_date = start_date + timedelta(days = number_days)
	start = start_date.strftime('%Y-%m-%d')
	end = end_date.strftime('%Y-%m-%d')

	select = "SELECT a.ticker, a.date, substr(a.period, 0, 4) FROM " + SCHEDULE_TABLE + " a"
	if tested_stocks_only:
		sql = select + " LEFT JOIN (SELECT DISTINCT ticker FROM " + TRADE_TABLE + ") b ON a.ticker = b.ticker WHERE b.ticker IS NOT NULL AND a.date >= :start and a.date < :end"
	else:
		sql = select + " WHERE a.date >= :start and a.date < :end"

	cur.execute(sql, {'start' : start, 'end' : end})

	rows = cur.fetchall()
	return [{'ticker':row[0], 'date':row[1], 'quarter': row[2]} for row in rows]


def backup_earning_history():
	msqlite.backup_table(EARNING_HISTORY_TABLE, None, conn)
	
def save_stock_info(records):
	print("Save {} stock records".format(len(records)))
	count_sql = "select count(*) from " + STOCK_TABLE
	print("Current stock record count:{}".format(query_single_value(count_sql)))


	for record in records:
		cur.execute("INSERT OR REPLACE INTO " + STOCK_TABLE + " values(:ticker, :name, :industry, :sector, :start_date, :size,  :exchange)", record)

	print("Record count:{}".format(query_single_value(count_sql)))

	conn.commit()


def save_price_history(ticker, records):
	print("save {} price history: {} days price".format(ticker, len(records)))
	count_sql = "select count(*) from " + PRICE_HISTORY_TABLE + " where ticker='" + ticker + "'"

	print("record count:{}".format(query_single_value(count_sql)))

	for record in records:
		record['ticker'] = ticker
		record['adj_close'] = ''
		cur.execute("INSERT OR REPLACE INTO " + PRICE_HISTORY_TABLE + " values(:ticker, :date, :open, :high, :low, :close, :volume, :adj_close)", record)

	print("record count:{}".format(query_single_value(count_sql)))

	conn.commit()

def save_earning_history(ticker, records):
	print("save {} earning history: {} earnings".format(ticker, len(records)))
	count_sql = "select count(*) from " + EARNING_HISTORY_TABLE + " where ticker='" + ticker + "'"

	print("record count:{}".format(query_single_value(count_sql)))

	for record in records:
		record['ticker'] = ticker
		cur.execute("insert or replace into " + EARNING_HISTORY_TABLE + " values(:ticker, :date, :estimate, :period, :reported, :surprise1, :surprise2)", record)

	print("record count:{}".format(query_single_value(count_sql)))

	conn.commit()

def query_single_value(sql, params = None):
	if(params):
		cur = conn.execute(sql, params)
	else:
		cur = conn.execute(sql)

	value = cur.fetchall()[0][0]
	cur.close()
	return value


def save_earning_schedule(list_schedule):
	msqlite.backup_table(SCHEDULE_TABLE, None, conn)
	for schedule in list_schedule:
		cur.execute("insert or replace into " + SCHEDULE_TABLE + "(ticker, date, eps, last_year_date, last_year_eps, period, numests, company ) values(:ticker,:date, :eps, :last_year_date, :last_year_eps, :month, :numests, :company)", schedule)

	conn.commit()

def save_test_trades(ticker, trades):
	trades.reset_index(inplace=True)
	print("\tDelete previous created {} test trades".format(ticker))
	conn.execute("delete from " + TRADE_TABLE + " where ticker=:ticker", {'ticker' : ticker})

	for index, trade in trades.iterrows():
		fields = trade.tolist()
		fields.insert(0, ticker)
		conn.execute("insert or replace into trades values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", fields)

	conn.commit();



def test_summary(ticker, quarter):
	params = {'ticker' : ticker, 'quarter' : quarter}

	cur.execute("select period from t.trades where ticker=:ticker and substr(period, 0, 4)=:quarter group by period", params)
	periods = cur.fetchall()
	total_periods = [period[0] for period in periods]

	params['profit2'] = 4
	cur.execute("select period from t.trades where ticker=:ticker and substr(period, 0, 4)=:quarter and profit2>:profit2 group by period", params)
	periods = cur.fetchall()
	winning_periods = [period[0] for period in periods]

	return {'total_periods' : total_periods, 'winning_periods' : winning_periods}



def find_profitable_stock_ratio_for_period(periods):
	result = [];
	for period in periods:
		cur2.execute("select count(*) from (select distinct ticker from trades where period=:period)", {"period" : period})
		total_stocks = cur2.fetchone()

		cur2.execute("select count(*) from (select distinct ticker from trades where period=:period and profit2 > 0)", {"period" : period})
		profitable_stocks = cur2.fetchone();

		result.append({'period' : period, 'count' : total_stocks[0], 'winning' : profitable_stocks[0]})

	return result

def find_profitable_stocks_for_period(period):
	cur2.execute("select distinct ticker from trades where profit2 > 0 and period =:period", {"period" : period})
	profitable_stocks = cur2.fetchall()
	return [stock[0] for stock in profitable_stocks]


def find_consective_winning_stocks(start_period, num_period):
	QUARTERS = ['Mar', 'Jun', 'Sep', 'Dec']
	quarter = start_period[:3]
	iquarter = QUARTERS.index(quarter)
	year = int(start_period[4:])

	periods = []
	for i in range(num_period):
		if iquarter >= 4:
			iquarter = 0
			year = year + 1
		
		period = QUARTERS[iquarter] + ' ' + str(year)
		periods.append(period)

		iquarter = iquarter + 1	

	print(periods)

	winning_stocks = None
	for period in periods:
		print (period)
		stocks = find_profitable_stocks_for_period(period)
		
		if winning_stocks is None:
			winning_stocks = set(stocks)
		else:
			winning_stocks = winning_stocks & set(stocks)
		
		print(len(winning_stocks))
		
def find_consective_winning_stocks_on_quarter(quarter, start_year, end_year):
	sql = "select ticker, count(*) from (select ticker, period, count(*)  from trades where profit2 > 0 and substr(period, 0, 4) = :quarter and substr(period, 5) >=:start_year and substr(period, 5) < :end_year group by ticker, period) group by ticker having count(*) = :years"
	years = int(end_year) - int(start_year)
	cur2.execute(sql, {'quarter' : quarter, 'start_year' : start_year, 'end_year' : end_year, 'years' : years})
	profitable_stocks = cur2.fetchall()
	return [stock[0] for stock in profitable_stocks]	


def test_trade_summary(tickers):
	tested_periods_count = "select ticker, count(*) as tested_periods from (select ticker, period from " + TRADE_TABLE + " group by ticker, period) group by ticker"
	win_periods_count = "select ticker, count(*) as win_periods from (select ticker, period from " + TRADE_TABLE + " where profit > 0 group by ticker, period) group by ticker"
	
	sql = "SELECT a.ticker, b.win_periods, a.tested_periods FROM ({}) a, ({}) b ON a.ticker = b.ticker".format(tested_periods_count, win_periods_count)
	rows = conn.execute(sql).fetchall()
	return [{'ticker' : row[0], 'win_periods' : row[1], 'tested_periods' : row[2]} for row in rows]



