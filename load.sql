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
	quarter varchar
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
	adj_close float
	constraint earning_pk primary key (ticker, date)	
);


--- Golden Repository
create table earning
(
	ticker varchar not null,
	date varchar,
	estimate varchar,
	period varchar not null,
	reported varchar,
	surprise1 varchar,
	surprise2 varchar,
	quarter varchar,

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
	adj_close float
	constraint earning_pk primary key (ticker, date)	
);