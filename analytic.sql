ATTACH DATABASE schedule AS s;
ATTACH DATABASE trades AS t;
ATTACH DATABASE analytics AS a;


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
SELECT a.ticker, a.date, substr(a.period, 0, 4) as quarter
FROM s.schedule a, a.quarter_win b
WHERE date > '2017-03-18' and a.ticker = b.ticker and substr(a.period, 0, 4) = b.quarter and b.total_periods_tested = b.win_periods_tested
ORDER BY date

----Max profit over min holding date stragety for upcoming earning report stocks which have all winning tested period
SELECT * 
FROM (
  ---Finds min holding date 
  SELECT a.ticker as ticker, a.date as date, a.quarter as quarter, b.period as period, b.buy_days as buy_days, b.sell_days as sell_days, b.profit2 as profit2
  FROM
  (
    --- select upcoming earning reporting stocks which all winning tested quarter 
    SELECT a.ticker as ticker, a.date as date, substr(a.period, 0, 4) as quarter
    FROM s.schedule a, a.quarter_win b
    WHERE date > '2017-03-18' and a.ticker = b.ticker and substr(a.period, 0, 4) = b.quarter and b.total_periods_tested = b.win_periods_tested
    ORDER BY date
  ) a, t.trades b
  WHERE a.ticker = b.ticker and a.quarter = substr(b.period, 0, 4) and b.profit > 0
  GROUP BY a.ticker, b.period
  HAVING (b.sell_days - b.buy_days) = min(b.sell_days - b.buy_days)
)
GROUP BY ticker 
HAVING profit2 = max(profit2)
ORDER BY date
