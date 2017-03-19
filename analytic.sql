ATTACH DATABASE schedule AS s;
ATTACH DATABASE trades AS t;

---get all stocks which have been back tested
create table target_stocks as 
select a.*, substr(a.period, 0, 4) as quarter
from s.schedule a
left join t.stocks b
on a.ticker = b.ticker
where b.ticker is not null;


---get all back test trades for target stocks
create table trades as 
	select a.*
	from t.trades a, target_stocks b 
	where a.ticker = b.ticker;


---find how many winning periods vs tested all periods and save result into wins table
create table a.wins as 
select a.ticker, b.win_periods, a.tested_periods
from (select ticker, count(*) as tested_periods from (select ticker, period from t.trades group by ticker, period) group by ticker) a
left join (select ticker, count(*) as win_periods from (select ticker, period from t.trades where profit > 0 group by ticker, period) group by ticker) b
on a.ticker = b.ticker

--- 1. Find how many periods tested for each quarter for each stock
select ticker, substr(period, 0, 4) as quarter, count(*) as total_periods_tested
FROM (select ticker, period from t.trades group by ticker, period ) 
GROUP BY ticker, quarter

--- 2 Find how many win periods for each quarter for each stock
select ticker, substr(period, 0, 4) as quarter, count(*) as win_periods_tested
FROM (select ticker, period from t.trades where profit > 0 group by ticker, period ) 
GROUP BY ticker, quarter

--- 3. Combin 1 and 2
CREATE TABLE a.quarter_win AS
SELECT a.ticker, a.quarter, a.total_periods_tested, b.win_periods_tested
FROM (
select ticker, substr(period, 0, 4) as quarter, count(*) as total_periods_tested
FROM (select ticker, period from t.trades group by ticker, period ) 
GROUP BY ticker, quarter
) a
LEFT JOIN (
select ticker, substr(period, 0, 4) as quarter, count(*) as win_periods_tested
FROM (select ticker, period from t.trades where profit > 0 group by ticker, period ) 
GROUP BY ticker, quarter
) b
ON a.ticker = b.ticker and a.quarter = b.quarter

---Find target trading stocks whose earning report scheduled released in next 20 days
---1. all wins over all tested period
SELECT a.ticker, a.date, a.period, b.win_periods, b.tested_periods
FROM s.schedule a
LEFT JOIN a.wins b
ON a.ticker = b.ticker
WHERE a.date > '2017-03-18' and b.ticker IS NOT NULL and b.tested_periods - b.win_periods < 2  and b.tested_periods > 5
ORDER BY a.date
---2. all wins over coming earning quarter



---find stocks wins all tested periods
select * from wins where tested_periods = win_periods and tested_periods > 10;

---Trading signals
create table target_stocks as select a.ticker as ticker, a.date as date, a.period as period from stocks a, (select ticker from wins where tested_periods = win_periods and tested_periods > 10) b where a.ticker = b.ticker and a.date > '20170313'

create table target_stocks_test_trades as 
select a.* 
from trades a, target_stocks b
where a.ticker = b.ticker
	and a.profit > 0
	and substr(a.period, 0, 4) = substr(b.period, 0, 4) 


---Max Profit over Min holding 
select a.*, b.date from 
(select ticker, buy_days, sell_days, profit2 from (select * from target_stocks_test_trades group by ticker, period having (sell_days - buy_days) = min(sell_days - buy_days)) group by ticker having profit2 = max(profit2)) a,
target_stocks b
where a.ticker = b.ticker;


---Min holding ov Max profit
select a.*, b.date from 
(select ticker, buy_days, sell_days, profit2 from (select * from target_stocks_test_trades group by ticker having profit2 = max(profit2)) group by ticker having (sell_days - buy_days) = min(sell_days - buy_days)) a,
target_stocks b
where a.ticker = b.ticker;



---Max profit
select * from trades where ticker = 'adbe' and profit > 0 and substr(period, 0, 4) = 'Feb' group by period having max(profit2)







 and substr(b.month, 0, 4) = substr(a.month, 0, 4);


select date, substr(date, 7, 4) || substr(date, 0, 3) || substr(date, 4, 2)  as date2  from target_stocks;


CREATE TABLE IF NOT EXISTS "russell3000"(
  "#" TEXT,
  "ticker" TEXT,
  "StartDate" TEXT,
  "Size(MB)" TEXT,
  "Description" TEXT,
  "Exchange" TEXT,
  "Industry" TEXT,
  "Sector" TEXT
);

