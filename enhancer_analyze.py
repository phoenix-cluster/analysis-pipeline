#!/usr/bin/python3
"""
This program match a project's spectra to the PRIDE Cluster spectral library, to detect the
(low confident) doubted PSMs,
(high confident) approved PSMs,
new PSMs,
and recommend better PSMs for some doubted PSMs.
Usage:
enhancer_analyze.py --project <projectId>
[--host <host_name>]
[--date <date>]
[(--loadfile | --loaddb)]
 enhancer_analyze.py (--help | --version)

Options:
-p, --project=<projectId>            project to be ananlyzed, the files should be putted in this directory
--host=<host_name>                   The host phoenix  to store the data and analyze result
--date =<date>                       The date to specify the tables
--loadfile                           If set, load spectra lib search result from pep.xml file.
--loaddb                             If set, load spectra lib search result from phoenix db.
-h, --help                           Print this help message.
-v, --version                        Print the current version.
"""


import sys, os
import logging
import time
from docopt import docopt
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import retrieve_splib_result as retriever
import phoenix_import_util as phoenix
import statistics_util as stat_util



def main():
    arguments = docopt(__doc__, version='cluster_phoenix_importer 1.0 BETA')
    logging.basicConfig(filename='myapp.log', level=logging.INFO)

    project_id = arguments['--project']
    host = "localhost"
    if arguments['--host']:
        host = arguments['--host']

    if project_id == None:
        raise Exception("No project id inputed, failed to do the analysis.")

    logging.info("Start to calculate project: " + project_id)

    date = time.strftime("%Y%m%d") #date is for choose the tables which has date as suffix

    if arguments['--date']:
        date = arguments['--date']

    # retrive from spectraST search result files
    start = time.clock()
    output_to_file = False
    lib_search_results = retriever.retrive_and_persist_to_file(project_id, output_to_file) #retrieve the library search results and export them to file/phoenix db
    elapsed = time.clock() - start
    logging.info("%s retriving lib search results takes time: %f"%(project_id, elapsed))

    # export search result to phoenix_db by building the whole big table
    start = time.clock()
    identified_spectra = phoenix.retrieve_identification_from_phoenix(project_id, host, None)
    cluster_data = phoenix.get_cluster_data(lib_search_results, host)
    phoenix.upsert_matched_psm_table(project_id, lib_search_results, identified_spectra, cluster_data, host, date)
    elapsed = time.clock() - start
    logging.info("%s phoenix persisting lib search results takes time: %f"%(project_id, elapsed))
    # #
    # # #analyze and export PSMs to file and phoenix_db
    start = time.clock()
    conf_sc_set = phoenix.export_sr_to_phoenix(project_id, lib_search_results, cluster_data, host)
    elapsed = time.clock() - start
    logging.info("%s analysis PSMs and persisting result to phoexnix-db takes time: %f"%(project_id, elapsed))

    #set thresholds and get statistics
    start = time.clock()
    phoenix.create_project_ana_record_table( host)
    thresholds = stat_util.default_thresholds
    phoenix.build_score_psm_table_new(project_id, thresholds, host, date)

    stat_util.create_views(project_id, thresholds, date, host)
    statistics_results = stat_util.calc_and_persist_statistics_data(project_id, host)
    elapsed = time.clock() - start
    logging.info("%s stastics calculation takes time: %f"%(project_id, elapsed))
    logging.info(statistics_results)

    logging.info('Finished')

if __name__ == "__main__":
    main()
