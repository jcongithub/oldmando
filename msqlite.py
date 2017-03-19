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


def get_table_ddl(table, conn):
	cur = conn.cursor()
	cur.execute("select sql from sqlite_master where name=:table", {'table':table})
	row = cur.fetchone()
	return row[0]

def diff_tables(table1, table2, pk, conn):
	table1_count = (conn.execute("SELECT count(*) FROM " + table1).fetchone())[0]
	table2_count = (conn.execute("SELECT count(*) FROM " + table2).fetchone())[0]
	print("{}:{} rows {}:{} rows".format(table1, table1_count, table2, table2_count))
	
	diff_table_name = make_diff_table_name(table1, table2)
	drop_table(diff_table_name)

	sql = rows_only_in_table1(table1, table2, pk)
	
	create_diff_table = "CREATE TABLE {} AS {}".format(diff_table_name, sql)
	conn.execute(create_diff_table)
	table1_only_count = table_row_count(diff_table_name, conn)

	## Insert rows only in table2
	sql = rows_only_in_table1(table2, table1, pk)
	insert_diff_table = "INSERT INTO {} {}".format(diff_table_name, sql)
	conn.execute(insert_diff_table)
	table2_only_count = table_row_count(diff_table_name, conn) - table1_only_count

	print("{} rows are only in {}".format(table1_only_count, table1))
	print("{} rows are only in {}".format(table2_only_count, table2))

	## Insert rows which are differnt in table1 and table2 into diff table
	columns = table_columns(table1, conn)
	sqls = make_sql_insert_diff_rows(table1, table2, pk, columns, diff_table_name)
	for sql in sqls:
		conn.execute(sql)

	conn.commit()


def table_row_count(table, conn):
	row = conn.execute("SELECT count(*) FROM " + table).fetchone()
	return row[0]

def drop_table(table):
	conn.execute("DROP TABLE IF EXISTS " + table)


def rows_only_in_table1(table1, table2, pk):
	table1_pk = ["a." + key for key in pk]
	table2_pk = ["b." + key for key in pk]	

	pk_equal_list = []
	for index in range(len(pk)):
		pk_equal_list.append(table1_pk[index] + "=" + table2_pk[index])

	on_list = " and ".join(pk_equal_list)
	where_List = " and ".join([key + " IS NULL" for key in table2_pk])
	
	sql = "SELECT '{}' as table_name, a.* FROM {} a LEFT JOIN {} b ON {} WHERE {}".format(table1, table1, table2, on_list, where_List)

	return sql;

def make_sql_insert_diff_rows(table1, table2, pk, columns, diff_table_name):
	table1_pk = ["a." + key for key in pk]
	table2_pk = ["b." + key for key in pk]	
	pk_equal_list = []
	for index in range(len(pk)):
		pk_equal_list.append(table1_pk[index] + "=" + table2_pk[index])
	
	pk_equal = " and ".join(pk_equal_list)

	non_pk_columns = list(set(columns) - set(pk))
	columns_not_equal = " OR ".join(["a.{} != b.{}".format(column, column) for column in non_pk_columns])

	sqls = []
	sqls.append("INSERT INTO {} SELECT '{}' as table_name, a.* FROM {} a, {} b WHERE {} AND ({})".format(diff_table_name, table1, table1, table2, pk_equal, columns_not_equal))
	sqls.append("INSERT INTO {} SELECT '{}' as table_name, b.* FROM {} a, {} b WHERE {} AND ({})".format(diff_table_name, table2, table1, table2, pk_equal, columns_not_equal))

	return sqls;

def table_columns(table, conn):
	rows = conn.execute("pragma table_info('{}')".format(table)).fetchall()
	return [row[1] for row in rows]	


def make_diff_table_name(table1, table2):
	return "diff__{}__{}".format(table1.replace(".", "_"), table2.replace(".", "_"))

def make_backup_table_name(table_name):
	today = datetime.now().strftime('%Y%m%d')
	backup_table_name = table_name + "_" + today
	return backup_table_name	


def make_tmp_table_name():
	return "tmp_" + str(int(round(time.time() * 1000)))



#if __name__ == '__main__':
#	conn = sqlite3.connect('db/history')	
#	diff_tables('earnings', 'earnings_20170317', ['ticker', 'period'], conn)
	#cur = conn.execute("select a.*, b.* from earnings a, earnings_20170317 b where a.ticker = b.ticker and a.period = b.period limit 5")
	#names = list(map(lambda x: x[0], cur.description))
	#print(names)
	#rows = cur.fetchall()
	#for row in rows:
	#	print(row)
