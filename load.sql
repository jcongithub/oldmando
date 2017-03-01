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

create table trade
(
	ticker varchar not null,
	earning_date varchar,
	month varchar,
	buy_days int, 
	sell_days int, 
	buy_date varchar,
	sell_date varchar,
	buy_price float,
	sell_price float, 
	profit float, 
	profit2 float
);



---make sure no duplicate earning report date
select ticker, date, count(*) as num from earning group by ticker, date having num > 1;

---
select * from earning a
join (select ticker, date, count(*) as num from earning group by ticker, date having num > 1) b
on a.ticker = b.ticker and a.date = b.date;