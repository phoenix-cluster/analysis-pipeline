#!/usr/bin/python3
"""
This program match a project's spectra to the PRIDE Cluster spectral library, to detect the
(low confident) doubted PSMs,
(high confident) approved PSMs,
new PSMs,
and recommend better PSMs for some doubted PSMs.
Usage:
enhancer_analyze.py --project <projectId>
[--date <date>]
[--minsize=<minClusterSize>]
[(--loadfile | --loaddb)]
 enhancer_analyze.py (--help | --version)

Options:
-p, --project=<projectId>            project to be ananlyzed, the files should be putted in this directory
-s, --minsize=<minClusterSize>   minimum cluster size to be matched.
--date =<date>                       The date to specify the tables
--loadfile                           If set, load spectra lib search result from pep.xml file.
--loaddb                             If set, load spectra lib search result from mysql_acc db.
-h, --help                           Print this help message.
-v, --version                        Print the current version.
"""


import sys, os
import logging
import time
from docopt import docopt
import glob

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import retrieve_splib_result as retriever
#import phoenix_import_util as phoenix
import mysql_storage_access as mysql_acc
import statistics_util as stat_util
import utils.build_cluster_csv as cluster_csv
import utils.score_psms as score_psms
import psm_util



def main():
    arguments = docopt(__doc__, version='enhancer_analyze 0.0.1')

    project_id = arguments['--project']
    min_cluster_size = arguments['--minsize'] or arguments['-s']
    min_cluster_size = int(min_cluster_size)

    if project_id == None:
        raise Exception("No project id inputed, failed to do the analysis.")

    logging.basicConfig(filename="%s_pipeline.log"%project_id, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logging.info("Start to calculate project: " + project_id)

    # date = time.strftime("%Y%m%d") + "3" #date is for choose the tables which has date as suffix
    date = ''
    if arguments['--date']:
        date = arguments['--date']

    # retrive from spectraST search result files
    start = time.clock()
    lib_search_results = None
    input_path = project_id + '/'
    sr_csv_file = project_id + '/' + project_id + 'lib_search_result.csv'
    try:
        lib_search_results = retriever.retrive_search_result(project_id, input_path, sr_csv_file) #retrieve the library search results and export them to file/mysql_acc db
    except Exception as err:
        logging.info("error in retriving spectraST search result file %s"%(err))

    elapsed = time.clock() - start
    logging.info("%s retriving lib search results takes time: %f"%(project_id, elapsed))

    # export search result to mysql_acc_db by building the whole big table
    start = time.clock()
    # psm_file = project_id + "/" + project_id + "_psm.csv"
#    spec_file = project_id + "/" + project_id + "_spec.csv"
    spec_files = glob.glob(project_id + "/*_spec.csv")
    psm_files = glob.glob(project_id + "/*_psm.csv")
    logging.info("start to read identification from csv")
    print("start to read identification from csv")
    identified_spectra  = psm_util.read_identification_from_csv(psm_files)
    if identified_spectra == None:
        identified_spectra = mysql_acc.retrieve_identification_from_db(project_id, None)
    else:
        mysql_acc.insert_psms_to_db_from_csv(project_id, identified_spectra, psm_files)

    mysql_acc.insert_spec_to_db_from_csv(project_id, spec_files) #specs also needs to be import because java pride xml importer don't import to phoenix any more

    cluster_data = cluster_csv.read_csv('/home/ubuntu/mingze/spec_lib_searching/phospho/clusters_min5.csv')
    if cluster_data == None:
        cluster_data = mysql_acc.get_cluster_data(lib_search_results)

    spec_match_detail_file = project_id + "/" + project_id + "_spec_match_details.csv"
    matched_spec_details_dict = psm_util.read_matched_spec_from_csv(spec_match_detail_file)
    if matched_spec_details_dict == None:
        matched_spec_details = psm_util.build_matched_spec(lib_search_results, identified_spectra, cluster_data)
        psm_util.write_matched_spec_to_csv(matched_spec_details, spec_match_detail_file)
        mysql_acc.upsert_matched_spec_table(project_id, matched_spec_details)
        matched_spec_details_dict = psm_util.read_matched_spec_from_csv(spec_match_detail_file)
    else:
        table_is_equal = retriever.table_is_equal_to_csv(project_id, matched_spec_details_dict)
        if not table_is_equal:
            matched_spec_details = psm_util.trans_matched_spec_to_list(matched_spec_details_dict)
            mysql_acc.upsert_matched_spec_table(project_id, matched_spec_details)
    elapsed = time.clock() - start
    logging.info("%s mysql_acc persisting lib search results takes time: %f"%(project_id, elapsed))
    # #
    # # #analyze and export PSMs to file and mysql_acc_db
    start = time.clock()
    # conf_sc_set = mysql_acc.export_sr_to_db(project_id, lib_search_results, cluster_data, matched_spec_details, host)
    elapsed = time.clock() - start
    logging.info("%s analysis PSMs and persisting result to phoexnix-db takes time: %f"%(project_id, elapsed))

    #set thresholds and get statistics
    start = time.clock()
    mysql_acc.create_project_ana_record_table()
    thresholds = stat_util.default_thresholds
    thresholds["cluster_size_threshold"] = min_cluster_size
    (p_score_psm_list, n_score_psm_list, new_psm_list) = score_psms.build_score_psm_list(cluster_data, thresholds, matched_spec_details_dict)
    mysql_acc.upsert_score_psm_table(project_id, p_score_psm_list, n_score_psm_list, new_psm_list)
    elapsed = time.clock() - start
    logging.info("%s build score psm table takes time: %f"%(project_id, elapsed))

    start = time.clock()
    stat_util.create_views(project_id, thresholds)
    statistics_results = stat_util.calc_and_persist_statistics_data(project_id, identified_spectra)
    elapsed = time.clock() - start
    logging.info("%s stastics calculation takes time: %f"%(project_id, elapsed))
    get_alpha = lambda x: x[0]
    logging.info(sorted(statistics_results.items(), key=get_alpha))

    logging.info('Finished')
    return 0

if __name__ == "__main__":
    main()
