""" import_splib_index.py

This tool import the scan number --> ClusterUniID map to MySQL DB 

Usage:
  import_splib_index.py --input=<input.mzXML> 
  import_splib_index.py (--help | --version)

Options:
  -i, --input=<input.mzXML>        Path to the result .pep.xml file to process.
  --tablename=[tablename]          Table name in MySQL db to store the spectra library search result.
                                   Default is the mzXML file name.
  --host=[host_name_of_db]         Host name or IP address of the MySQL server.
  -h, --help                       Print this help message.
  -v, --version                    Print the current version.

"""
import sys
import os
from docopt import docopt
import xml.etree.ElementTree as ET
import pymysql.cursors

def check_table(connection,table_name):
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
                    "scan_num int(15) COLLATE utf8_bin NOT NULL,"    + \
                    "cluster_id varchar(100) COLLATE utf8_bin NOT NULL,"    + \
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



def connect_and_check(mysql_host, table_name):
    #build the connection
    connection = pymysql.connect(host=mysql_host,
                            user='pride_cluster_t',
                            password='pride123',
                            db='pride_cluster_t',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
    print("Opened database successfully");
    #deal with the table name
    check_table(connection, table_name)
    return connection

def remove_mzxml_ext(path_name):
    return path_name[:-8]

def get_id_map(mzXML_path):
    ns_str = "{http://sashimi.sourceforge.net/schema_revision/mzXML_3.2}"
    tree = ET.parse(mzXML_path)
    root = tree.getroot()
    id_map = {}
    for scan in root.iterfind(ns_str + "msRun/" + ns_str + "scan"):
        scan_num = scan.attrib.get('num')
        for nameValue in scan.iterfind(ns_str + 'nameValue'):
            name = nameValue.attrib.get('name')
            id_value = nameValue.attrib.get('value')
            if name != 'ClusterUniID':
                raise Exception ("Got unexpected nameValue tag inside scan " + scan_num)
            id_map[scan_num] = id_value
    return(id_map)

def insert_to_db(connection, table_name, scan_num, cluster_id):
    """    
    "scan_num int(15) NOT NULL,"    + \
    "cluster_id varchar(100) COLLATE utf8_bin NOT NULL,"    + \
    """

    insert_db = "INSERT INTO `" + table_name +"`" +\
                "(scan_num, cluster_id) VALUES " +\
                "('" + scan_num + "','" + cluster_id + "')"

    with connection.cursor() as cursor:
        cursor.execute(insert_db)



def main():
    arguments = docopt(__doc__, version='import_splib_index.py 1.0 BETA')
    mzXML_path = arguments['--input'] or arguments['-i']
    
    table_name = "test_spec_lib" + "_index"
#    if arguments['--tablename']:
#        table_name = remove_mzxml_ext(input_path) + "_index"
    
    connection = connect_and_check('localhost', table_name)
   
    scan_clusterid_map = get_id_map(mzXML_path)
    
    with open('test_scan_clusterid_map.tab','w') as o:
        for scan_num in scan_clusterid_map.keys():
            o.write("%s\t%s" % (scan_num,scan_clusterid_map.get(scan_num))
#    for scan_num in scan_clusterid_map.keys():
#        insert_to_db(connection, table_name, scan_num, scan_clusterid_map.get(scan_num))
#    connection.commit()

if __name__ == "__main__":
    main()
