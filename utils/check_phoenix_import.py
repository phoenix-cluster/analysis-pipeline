"""
This program check if a project has imported the csv files in to phoenix well
Usage:
check_phoenix_import.py --project <projectId>
[--host <host_name>]
check_phoenix_import.py (--help | --version)

Options:
-p, --project=<projectId>            project to be checked
--host=<host_name>                   The host phoenix  to store the data and analyze result
-h, --help                           Print this help message.
-v, --version                        Print the current version.
"""

import phoenixdb
from docopt import docopt
import os, sys, re, json
import pandas as pd

#file_dir = os.getcwd()
sys.path.append("/home/ubuntu/mingze/tools/spectra-library-analysis")


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
        for r in rs:
            results.append(r)
            # print(r)
            # for item in r:
            #     print(item)
            # line = ",".join(r)
            # lines.append(line)
    return results

def check(project_id, conn):
    psms_csv = "%s_psm.csv"%project_id
    spec_match_csv = "%s_spec_match_details.csv"%project_id

    psms_in_csv = pd.read_csv(psms_csv, sep='\\t', header=0)
    spec_match_details = pd.read_csv(spec_match_csv, sep='\\t', header=0)

    sql_str = "select count(*) from T_%s_PSM "%project_id
    identified_no = execute_sql(sql_str, conn)[0][0]
    print("\n\n========%s========"%project_id)

    if identified_no != len(psms_in_csv):
        print("ERROR! identified_no in phoenixdb is not equal to csv file: %d != %d"%(identified_no,len(psms_in_csv)))
    else:
        print("identified_no in phoenixdb is equal to csv file: %d == %d"%(identified_no,len(psms_in_csv)))

    sql_str = "select count(*) from T_%s_SPEC_CLUSTER_MATCH"%project_id
    spec_match_no = execute_sql(sql_str, conn)[0][0]
    print(spec_match_no)
    if spec_match_no != len(spec_match_details):
        print("ERROR! spec_match_no in phoenixdb is not equal to csv file: %d != %d"%(spec_match_no,len(spec_match_details)))
    else:
        print("spec_match_no in phoenixdb is equal to csv file: %d == %d"%(spec_match_no,len(spec_match_details)))

def main():
    arguments = docopt(__doc__, version='cluster_phoenix_importer 1.0 BETA')

    project_id = arguments['--project']
    host = "localhost"
    if arguments['--host']:
        host = arguments['--host']

    if project_id == None:
        raise Exception("No project id inputed, failed to do the analysis.")
    conn = get_conn(host)
    check(project_id, conn)
    conn.close()

#conn = get_conn("localhost")
#command = input("Enter the phoenix sql string you want to execute, input exit to quit\n")
#while(command != 'exit'):
#    execute_sql(command, conn)
#    command = input("Enter the phoenix sql string you want to execute, input exit to quit\n")


if __name__ == "__main__":
    main()
