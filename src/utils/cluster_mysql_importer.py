"""
cluster_mysql_importer

Command line interface to the spectra-cluster mysql importer. This tool import all listed  
clustering files into mysql as a sigle release table. 

Usage:
  cluster_mysql_importer.py --input <path_to_clustering_files>
                       [--min_size <size>] 
                       [--min_ratio <ratio>]
                       [--min_identified <spectra>]
                       [--table_name <table_name>] 
                       [--host <host_name>] 
                       [(--over_write_table)] 
                       [(--only_identified | --only_unidentified)]
  cluster_mysql_importer.py (--help | --version)

Options:
  -i, --input=<path_to_clustering_files>   Path to the directory with .clustering result files to process.
  --min_size=<size>                    The minimum size of a cluster to be reported. [default: 2]
  --min_ratio=<ratio>                  The minimum ratio a cluster must have to be reported.
  --min_identified=<spectra>           May specify the minimum number of identified spectra a cluster must have.
  --table_name=<table_name>            The table to store this cluster release 
  --host=<host_name>                   The host mysql  to store this cluster release 
  --over_write_table                   If set, the table will be over write directly.
  --only_identified                    If set, only identified spectra will be reported.
  --only_unidentified                  If set, only unidentified spectra will be reported.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.
"""

import sys
import os

#sys.path.insert(0, "./py-venv/lib/python3.6/site-packages")

import glob
from docopt import docopt
    
# make the spectra_cluster packages available
package_path = os.path.abspath(os.path.split(sys.argv[0])[0]) + os.path.sep + ".." + os.path.sep + ".."
sys.path.insert(0, package_path)

import cluster_mysql_importer_analyser as cluster_mysql_importer
import spectra_cluster.clustering_parser as clustering_parser


def create_analyser(arguments):
    """
    Creates an comparer analyser based on the command line
    parameters.
    :param arguments: The command line parameters
    :return: An Comparer object
    """
    analyser = cluster_mysql_importer.ClusterMySqlImporter()

    if arguments["--only_identified"]:
        analyser.add_to_unidentified = False

    if arguments["--only_unidentified"]:
        analyser.add_to_identified = False

    if arguments["--min_size"]:
        analyser.min_size = int(arguments["--min_size"])

    if arguments["--min_ratio"]:
        analyser.min_ratio = float(arguments["--min_ratio"])
    
    if arguments["--table_name"]:
        analyser.table_name = arguments["--table_name"]

    if arguments["--min_identified"] is not None:
        analyser.min_identified_spectra = int(arguments["--min_identified"])

    if arguments["--over_write_table"]:
        analyser.over_write_table = True 

    if arguments["--host"] is not None:
        analyser.mysql_host = arguments["--host"]

    return analyser

def main():
    """
    Primary entry function for the CLI.
    :return:
    """
    arguments = docopt(__doc__, version='cluster_mysql_importer 1.0 BETA')
    print(arguments)
#    sys.exit(1)

    # create the cluster comparer based on the settings
    analyser = create_analyser(arguments)
    analyser.connect_and_check()

    # make sure the input path exists and has .clustering files
    input_path = arguments['--input']
    if os.path.isfile(input_path):
        clustering_file = input_path
        parser0 = clustering_parser.ClusteringParser(clustering_file)
        for cluster in parser0:
            analyser.process_cluster(cluster)
    	# do the  importing to database 
        analyser.import_afile() 
#        analyser.update_clusters_afile()  #too slow to update a huge table
        analyser.clear() 
        print("Done importing of " + clustering_file)
        # analyser.import_projects()
        analyser.close_db()
        return
    
    #else:    
    clustering_files = glob.glob(input_path + "/*.clustering")
    if len(clustering_files) < 1:
        print("Error: Cannot find .clustering in path '" + input_path+ "'")
        sys.exit(1)
        
    for clustering_file in clustering_files:
        if not os.path.isfile(clustering_file):      
            print("Error: this clustering file is not a file '" + clustering_file + "'")
            sys.exit(1)


    print("Parsing input .clustering files...")
    # process all clustering files
    for clustering_file in clustering_files:
        parser0 = clustering_parser.ClusteringParser(clustering_file)
        for cluster in parser0:
            analyser.process_cluster(cluster)
    	# do the  importing to databasef
        analyser.import_afile() 
#        analyser.update_clusters_afile()  #too slow to update a huge table
        analyser.clear() 
        print("Done importing of " + clustering_file)
    # analyser.import_projects()
    analyser.close_db()



if __name__ == "__main__":
    main()
