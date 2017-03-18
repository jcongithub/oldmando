import sqlite3
import time
from datetime import datetime

def backup_table(src, dest, conn, auto_commit=True):
	if dest is None:
		dest = make_backup_table_name(src)

	print("backup table:{} to {}".format(src, dest))
	print("droping table:{}".format(dest))

	cur = conn.cursor()
	cur.execute("DROP TABLE IF EXISTS " + dest)
	cur.execute("create table " + dest + " as select * from " + src)

	if auto_commit:
		conn.commit()

	print("table:{} backed up to:{}".format(src, dest))

def rename_column(table, old_column, new_column, conn):
	tmp_table_name = make_tmp_table_name()	
	backup_table(table, tmp_table_name, conn, False)
	create_sql = get_table_ddl(table, conn)
	create_sql = create_sql.replace(old_column, new_column)

	#drop original table
	conn.execute("DROP TABLE IF EXISTS " + table)
	#create new table with new column name
	conn.execute(create_sql)

	#copy old data from tmp table
	conn.execute("INSERT INTO " + table + " SELECT * FROM " + tmp_table_name)
	
	#drop tmp table
	conn.execute("DROP TABLE IF EXISTS " + tmp_table_name)

	conn.commit()

def make_backup_table_name(table_name):
	today = datetime.now().strftime('%Y%m%d')
	backup_table_name = table_name + "_" + today
	return backup_table_name	

def make_tmp_table_name():
	return "tmp_" + str(int(round(time.time() * 1000)))

def get_table_ddl(table, conn):
	cur = conn.cursor()
	cur.execute("select sql from sqlite_master where name=:table", {'table':table})
	row = cur.fetchone()
	return row[0]

def diff_tables(table1, table2, pk, conn):
	table1_count = (conn.execute("SELECT count(*) FROM " + table1).fetchone())[0]
	table2_count = (conn.execute("SELECT count(*) FROM " + table2).fetchone())[0]
	print("{}:{} rows {}:{} rows".format(table1, table1_count, table2, table2_count))
	
	table1_pk = ["a." + key for key in pk]
	table2_pk = ["b." + key for key in pk]	
	#column_list = ", ".join(table1_pk)
	column_list = "a.*"
	print("SELECT {}".format(column_list))

	pk_equal_list = []
	for index in range(len(pk)):
		print(index)
		pk_equal_list.append(table1_pk[index] + "=" + table2_pk[index])

	on_list = " and ".join(pk_equal_list)
	where_List = " and ".join([key + " IS NULL" for key in table2_pk])
	
	sql = "SELECT {} FROM {} a LEFT JOIN {} b ON {} WHERE {}".format(column_list, table1, table2, on_list, where_List)
	rows = conn.execute(sql).fetchall()
	print("{} records are only in {}".format(len(rows), table1))
	for row in rows:
		print(row)

	column_list = "b.*"
	where_List = " and ".join([key + " IS NULL" for key in table1_pk])
	sql = "SELECT {} FROM {} a RIGHT JOIN {} b ON {} WHERE {}".format(column_list, table1, table2, on_list, where_List)
	print("{} records are only in {}".format(len(rows), table2))
	for row in rows:
		print(row)


	


if __name__ == '__main__':
	conn = sqlite3.connect('db/history')	
	##diff_tables('earnings', 'earnings_20170317', ['ticker', 'period'], conn)
	cur = conn.execute("select a.*, b.* from earnings a, earnings_20170317 b where a.ticker = b.ticker and a.period = b.period limit 5")
	names = list(map(lambda x: x[0], cur.description))
	print(names)
	rows = cur.fetchall()
	for row in rows:
		print(row)


