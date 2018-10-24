""" identi_data_to_mysql.py

This tool import the target spectra's identification data into the mysql db. 

Usage:
  identi_data_to_mysql.py --input=<path_to_mgf_files> [--tablename=<tablename>] 
  identi_data_to_mysql.py (--help | --version)

Options:
  -i, --input=<path>        		Path to the mgf files to process.
  --tablename=[tablename]              Table name in MySQL db to store the spectra library search result.
  --host=[host_name_of_db]             Host name or IP address of the MySQL server.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.

"""
import sys
import os,glob
from docopt import docopt
import xml.etree.ElementTree as ET
import pymysql.cursors
from pyteomics import mgf
connection = None
table_name = 'PXD003292' + '_ident'

def check_table():
    global connection, table_name
    """
    Check the table name exists or not,
    create it if not exists,
    ask user if they want to overwrite the table if exists.
    """
    table_exists = None
    create_new = None
    over_write_table = True

    tb_exists = "SHOW TABLES LIKE '" + table_name + "';"
    # create a table
    tb_create = "CREATE TABLE `" + table_name + "` ("                     + \
                    "id int(15) NOT NULL AUTO_INCREMENT,"    + \
                    "spectra_title varchar(100) COLLATE utf8_bin NOT NULL,"    + \
                    "sequence varchar(100) COLLATE utf8_bin NOT NULL,"    + \
                    "PRIMARY KEY (id)" + ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
    print(tb_create)
    try:
        with connection.cursor() as cursor:
            cursor.execute(tb_exists)
            result = cursor.fetchone()
            connection.commit()
            if result != None :
                table_exists = True
   #except IOError as e:
            else:
                print("Table does not exists")
                table_exists = False
                create_new = True
            
            if table_exists and not over_write_table: 
                print("The table" + table_name + "is already exists, do you really want to overwrite it?")
                answer = input("please input yes | no:  ")
                while(answer != 'yes' and answer != 'no'):
                    answer = input("please input yes | no:")
                if answer == 'no':
                    print("Going to exit.")
                    sys.exit(0)
                else:
                    create_new = True 

            if over_write_table or create_new :
                if table_exists:
                    print("Start droping the tables")
                    cursor.execute("DROP TABLE IF EXISTS `" + table_name + "`;")
                cursor.execute(tb_create)
                connection.commit()
    finally:
        print ("checked table")



def connect_and_check(mysql_host):
	#build the connection
    global connection, table_name
    connection = pymysql.connect(host=mysql_host,
                            user='pride_cluster_t',
                            password='pride123',
                            db='pride_cluster_t',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
    print("Opened database successfully");
    #deal with the table name
    check_table()

def insert_to_db(title,seq):
    insert_db = "INSERT INTO `" + table_name + "` (spectra_title, sequence) VALUES " +\
                "('" + title + "','" + seq + "')"
    with connection.cursor() as cursor:
        cursor.execute(insert_db)


def main():
    global connection, table_name

    arguments = docopt(__doc__, version='identi_data_to_mysql.py 1.0 BETA')
    mgf_path = arguments['--input'] or arguments['-i']
    if arguments['--tablename']:
        table_name = arguments['--tablename']

    connect_and_check('localhost')

    for file in os.listdir(mgf_path):
        if not file.lower().endswith('.mgf'):
            continue
        for spectrum in mgf.read(mgf_path + "/" + file):
            params = spectrum.get('params')
            title = params.get('title')
            seq = params.get('seq')
            if seq == "" or seq == None:
                seq = "_UNID_"
            insert_to_db(title,seq)
        connection.commit()


    """
    'params': {'seq': 'FEDSLCK', 'charge': [2], 'taxonomy': '9606', 'user03': '4-MOD:00696,6-MOD:01090', 'title': 'id=PXD000021;PRIDE_Exp_Complete_Ac_27184.xml;spectrum=4970', 'pepmass': (489.68381, None)}
    """

if __name__ == "__main__":
    main()


