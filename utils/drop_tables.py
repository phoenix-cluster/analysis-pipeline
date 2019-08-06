import phoenixdb
import time
import math
import os, sys, re, json
import logging


par_dir = os.path.abspath("..")
print(par_dir)
sys.path.append(par_dir)
import mysql_storage_access as mysql_acc

"""
Get connection 
"""
def get_conn():
    #conn = phoenixdb.connect(database_url, autocommit=True)
    conn = mysql_acc.get_conn()
    return conn

def getInput():
    pass

def showtables(show_table_condition, conn):
    print("condition: %s"%(show_table_condition))

    #sql_str = 'select DISTINCT("TABLE_NAME") from SYSTEM.CATALOG'
    sql_str = "show tables like '%" + show_table_condition + "%'"
    sql_str = "SELECT table_name FROM information_schema.tables"
    table_list = list()
    pattern = re.compile("^.*" + show_table_condition + ".*$", re.IGNORECASE)
    with conn.cursor() as cursor:
        cursor.execute(sql_str)
        rs = cursor.fetchall()
        for r in rs:
            if pattern.match(str(r[0])):
                table_list.append(str(r[0]))
    return table_list

def droptables(show_table_condition, conn):
    table_list = showtables(show_table_condition, conn)
    print("Found %d tables by your condition %s:\n" % (len(table_list), show_table_condition))
    print("\n".join(table_list))
    user_input = input("Do you really want to drop them all? yes/no\n")
    if user_input == 'yes':
        execute_droping(table_list, conn)
    else :
        return

def execute_droping(table_list, conn):
    try:
        with conn.cursor() as cursor:
            for table_name in table_list:
                drop_table_sql = "DROP TABLE  IF EXISTS " + table_name
                if table_name.startswith("V_"):
                    drop_table_sql = "DROP VIEW IF EXISTS " + table_name
                print(drop_table_sql)
                cursor.execute(drop_table_sql)
    except Exception as e:
        print(e)

conn = get_conn()
user_input = ''
while user_input != 'exit':
    user_input = input("\nPlease input the command like:\nshow T_PXD0000\ndrop T_PXD0000\n ============\n")
    if(user_input.startswith("show ")):
        show_table_condition = user_input.replace("show ",'')
        print(show_table_condition)
        table_list = showtables(show_table_condition, conn)
        print("\n".join(table_list))
    if(user_input.startswith("drop ")):
        show_table_condition = user_input.replace("drop ",'')
        droptables(show_table_condition, conn)
