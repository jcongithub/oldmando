--- Load database
create table earning
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

create table price
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

create table earning_schedule
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


---make sure no duplicate earning report date
select ticker, date, count(*) as num from earning group by ticker, date having num > 1;

---
select * from earning a
join (select ticker, date, count(*) as num from earning group by ticker, date having num > 1) b
on a.ticker = b.ticker and a.date = b.date;