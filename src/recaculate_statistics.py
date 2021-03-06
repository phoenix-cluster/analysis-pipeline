#!/usr/bin/python3
"""
This program recalculate a project's statistics results, by recreate the views
Usage:
recaculate_statistics.py --project <projectId> --minsize <min_cluster_size>
[--host <host_name>]

Options:
-p, --project=<projectId>            project to be ananlyzed, the files should be putted in this directory
-s, --minsize=<min_cluster_size>            project to be ananlyzed, the files should be putted in this directory
-h, --help                           Print this help message.
-v, --version                        Print the current version.
"""


import sys, os
#sys.path.insert(0, "./py-venv/lib/python3.6/site-packages")
import logging
import time
from docopt import docopt
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import statistics_util as stat_util
import psm_util



def main():
    arguments = docopt(__doc__, version='cluster_phoenix_importer 1.0 BETA')

    project_id = arguments['--project']
    min_cluster_size = arguments['--minsize'] or arguments['-s']
    min_cluster_size = int(min_cluster_size)


    if project_id == None:
        raise Exception("No project id inputed, failed to do the analysis.")

    logging.basicConfig(filename="%s.log"%project_id, level=logging.INFO)
    logging.info("Start to recalculate statistics for project: " + project_id)
    thresholds = stat_util.default_thresholds
    thresholds["cluster_size_threshold"] = min_cluster_size
    start = time.clock()
    stat_util.create_views(project_id, thresholds)

    print("start to read identification from csv")
    psm_file = project_id + "/" + project_id + "_psm.csv"
    identified_spectra  = psm_util.read_identification_from_csv(psm_file)

    statistics_results = stat_util.calc_and_persist_statistics_data(project_id, identified_spectra)
    elapsed = time.clock() - start
    logging.info("%s stastics calculation takes time: %f"%(project_id, elapsed))
    logging.info(statistics_results)

    logging.info('Finished')

if __name__ == "__main__":
    main()
