#!/usr/bin/python3
"""
This program filter down the old match data with new filtered clusters
Usage:
filter_search_result.py --project <projectId>
filter_search_result.py (--help | --version)

Options:
-p, --project=<projectId>            project to be ananlyzed, the files should be putted in this directory
-h, --help                           Print this help message.
-v, --version                        Print the current version.
"""

from docopt import docopt
import csv
import pandas as pd
import os


def filster_down(projectid, filtered_cluster_id_file):
    lib_search_result_file = os.path.join(projectid, "%slib_search_result.csv"%(projectid))
    lib_search_details_file = os.path.join(projectid, "%s_spec_match_details.csv"%(projectid))

    cluster_ids = list()
    with open(filtered_cluster_id_file) as file:
        cluster_ids = file.read().splitlines()

    lib_search_result = pd.read_csv(lib_search_result_file)
    os.rename(lib_search_result_file, lib_search_result_file + ".bak")
    lib_search_result_filtered = lib_search_result[lib_search_result['lib_spec_id'].isin(cluster_ids)]
    lib_search_result_filtered.to_csv(lib_search_result_file, index=False)

    lib_search_details = pd.read_csv(lib_search_details_file)
    os.rename(lib_search_details_file, lib_search_details_file + ".bak")
    lib_search_details_filtered = lib_search_details[lib_search_details['cluster_id'].isin(cluster_ids)]
    lib_search_details_filtered.to_csv(lib_search_details_file, index=False)

def main():
    arguments = docopt(__doc__, version='enhancer_analyze 0.0.1')
    project_id = arguments['--project']
    filtered_cluster_id_file = "/home/ubuntu/mingze/testhbase/data/201504/all_mintic0.2_minsize_5_clusterid.csv"
    filster_down(project_id, filtered_cluster_id_file=filtered_cluster_id_file)

if __name__ == "__main__":
    main()
