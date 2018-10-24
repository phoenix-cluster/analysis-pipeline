import phoenixdb
import time
import math
import os, sys, re, json
import logging

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)


"""
Get connection 
"""

def get_conn(host):
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    return conn



def execute_sql(sql_str, conn):
    """
    execute the sql string and print the output
    """

    with conn.cursor() as cursor:
        cursor.execute(sql_str)
        rs = cursor.fetchall()
        results = list()
        print("changed")
        for r in rs:
            results.append(r)
            # print(r)
            # for item in r:
            #     print(item)
            # line = ",".join(r)
            # lines.append(line)
    return results


#conn = get_conn("localhost")
#command = input("Enter the phoenix sql string you want to execute, input exit to quit\n")
#while(command != 'exit'):
#    execute_sql(command, conn)
#    command = input("Enter the phoenix sql string you want to execute, input exit to quit\n")
