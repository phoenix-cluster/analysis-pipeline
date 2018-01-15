#!/usr/bin/python3
"""
This program match a project's spectra to the PRIDE Cluster spectral library, to detect the
(low confident) doubted PSMs,
(high confident) approved PSMs,
new PSMs,
and recommend better PSMs for some doubted PSMs.
"""
import sys, os
import logging
import time
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import retrieve_splib_result as retriever
import phoenix_import_util as phoenix
import statistics_util as stat_util



def main():
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    logging.info('Started')
    project_id = sys.argv[1]
    host = "localhost"
    if project_id == None:
        raise Exception("No project id inputed, failed to do the analysis.")
    print("Start to calculate project: " + project_id)

    output_to_file = False
    lib_search_results = retriever.retrive_and_persist_to_file(project_id, output_to_file) #retrieve the library search results and export them to file/phoenix db
    identified_spectra = phoenix.retrieve_identification_from_phoenix(project_id, host, None)
    cluster_data = phoenix.get_cluster_data(lib_search_results, host)

    #persisit analyzed PSMs in file and phoenix_db
    phoenix.export_sr_to_phoenix(project_id, lib_search_results, identified_spectra, cluster_data, host)

    #set thresholds and get statistics
    date = time.strftime("%Y%m%d") #date is for choose the tables which has date as suffix
    thresholds = stat_util.default_threshods
    stat_util.set_threshold(project_id, thresholds, date, host)
    statistics_results = stat_util.get_statistics_data(project_id, host)
    print(statistics_results)

    logging.info('Finished')

if __name__ == "__main__":
    main()
