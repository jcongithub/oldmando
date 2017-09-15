--- Load database
create table earnings
(
	ticker varchar not null,
	date varchar,
	estimate varchar,
	period varchar not null,
	reported varchar,
	surprise1 varchar,
	surprise2 varchar,
	constraint earning_pk primary key (ticker, period)
);

create table prices
(
	ticker varchar not null,
	date varchar not null,
	open float,
	high float,
	low float,
	close float,
	volume int,
	adj_close float,
	constraint earning_pk primary key (ticker, date)	
);

create table trades
(
	ticker varchar not null,
	earning_date varchar,
	period varchar,
	buy_days int, 
	sell_days int, 
	buy_date varchar,
	sell_date varchar,
	buy_price float,
	sell_price float, 
	profit float, 
	profit2 float
);

create table schedule
(   ticker varchar not null,
	date   varchar,
	eps    varchar,
	last_year_date varchar,
	last_year_eps  varchar,
	month  varchar,
	numests int,
	company varchar,
	constraint schedule_pk primary key (ticker)
);

create table winning_quarters
(
	ticker varchar not null,
	quarter varchar not null,
	total int,
	wins int,
	constraint winning_quarters_pk primary key (ticker, quarter)	
);

---make sure no duplicate earning report date
select ticker, date, count(*) as num from earning group by ticker, date having num > 1;

---
select * from earning a
join (select ticker, date, count(*) as num from earning group by ticker, date having num > 1) b
on a.ticker = b.ticker and a.date = b.date;

---1. Find how many earning tested for each quarter each stock
select ticker, substr(period, 0, 4) as quarter, count(*) as total
from (select ticker, period from trades group by ticker, period) 
group by ticker, quarter;

---2. Find stocks which stock consectively wins for N times for the given quarter from YEAR
select ticker, substr(period, 0, 4) as quarter, count(*) as wins
from (select ticker, period from trades where profit2 > 0 group by ticker, period) 
group by ticker, quarter;

---3 combine 1 and 2
INSERT INTO winning_quarters(ticker, quarter, total, wins)
select a.ticker, a.quarter, a.total, b.wins
from 
(
	select ticker, substr(period, 0, 4) as quarter, count(*) as total
	from (select ticker, period from trades group by ticker, period) 
	group by ticker, quarter
) a, 
(
	select ticker, substr(period, 0, 4) as quarter, count(*) as wins
	from (select ticker, period from trades where profit2 > 0 group by ticker, period) 
	group by ticker, quarter
)b
WHERE a.ticker = b.ticker AND a.quarter = b.quarter

---4. Verify (3)
SELECT a.* 
FROM trades a, 
(
	SELECT ticker, quarter FROM winning_quarters WHERE quarter = 'Dec' AND total > 5 AND total = wins
) b
WHERE a.ticker = b.ticker 
	AND substr(a.period, 0, 4) = b.quarter
	AND profit2 <= 0

---5. Find earning schedule for winning stocks 
SELECT a.ticker, a.quarter, b.date 
FROM winning_quarters a,
	 s.schedule b 
WHERE a.quarter = 'Dec'
	AND a.total > 5 
	AND a.total = wins
	AND a.ticker = b.ticker
