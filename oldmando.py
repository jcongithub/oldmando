#!/usr/bin/python

import sqlite3
conn = sqlite3.connect('test.db')
#conn.execute('''create table users(
#    id int primary key not null,
#    name vchar(16) not null,
#    pass vchar(16) not null,
#    creation_time datetime default current_timestamp
#);''')

conn.execut()
print "Opened database successfully";
