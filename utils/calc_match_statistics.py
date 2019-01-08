""" calc_match_statistics.py

This tool calculate the match statistics of each project.

Usage:
  calc_match_statistics.py --project=<project_id>
  calc_match_statistics.py (--help | --version )

Options:
  -p, --project=<project_id>       project_id to be processed.
  -h, --help                       Print this help message.
  -v, --version                    Print the current version.
"""


import csv,pymysql,pandas
import os,sys,time
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
parent_dir = os.path.dirname(__file__) + "/.."
sys.path.append(parent_dir)
import mysql_storage_access as mysql_acc
from docopt import docopt

# while True:
#   print(sys.path)
#   time.sleep(1)

"""
Read matched cluster data from mysql db tables
"""
def get_cluster_data(match_details):
    cluster_data = dict()
    conn = mysql_acc.get_conn()
    cursor = conn.cursor()
    for index, row in match_details.iterrows():
        spec_title = row.get('spec_title')
        cluster_id = row.get('cluster_id')
        cluster_query_sql = "SELECT CLUSTER_RATIO, N_ID, CONF_SC, SEQUENCES_RATIOS, SPECTRA_TITLES FROM V_CLUSTER WHERE CLUSTER_ID = '" + cluster_id + "'"
#         print(cluster_query_sql)
#         return 0
        cursor.execute(cluster_query_sql)
        result = cursor.fetchone()
        cluster = dict()
        cluster['ratio'] = result[0]
        cluster['size'] = result[1]
        cluster['conf_sc'] = result[2]
        cluster['seqs_ratios'] = result[3]
        cluster['spectra_titles'] = result[4]
        cluster_data[cluster_id] = cluster
    cursor.close()
    conn.close()
    return cluster_data

"""
get cluster id which contains this spectrum
"""
def get_spec_in_which_cluster(spec_title, project_id):
    conn = mysql_acc.get_conn()
    cursor = conn.cursor()
    cluster_query_sql = "SELECT CLUSTER_FK FROM T_CLUSTER_SPEC_%s WHERE SPECTRUM_TITLE = '%s'"%(project_id, spec_title)
    cursor.execute(cluster_query_sql)
    result = cursor.fetchone()
    if result != None:
        cluster_id = result[0]
    else:
        cluster_id = ''
    cursor.close()
    conn.close()
    return cluster_id

"""
get the original identified peptide for this spectrum
"""
def get_origin_peptide(spec, project_id):
    conn = mysql_acc.get_conn()
    cursor = conn.cursor()
    query_sql = "SELECT PEPTIDE_SEQUENCE FROM T_%s_PSM WHERE SPECTRUM_TITLE='%s'"%(project_id, spec)
    cursor.execute(query_sql)
    result = cursor.fetchone()
    if result != None:
        peptide = result[0]
    else:
        peptide = ''
    cursor.close()
    conn.close()
    return peptide


import json

"""
get the main peptide in this cluster, which is the sequence with the highest ratio
"""
def get_main_peptide(clusters, cluster_id):
    cluster = clusters.get(cluster_id)
    str1 = cluster.get("seqs_ratios").replace("'", "\"")
    sequence_with_ratio = json.loads(str1)
    max_ratio = 0
    max_ratio_seq = ''
    for seq in sequence_with_ratio.keys():
        if sequence_with_ratio.get(seq) > max_ratio:
            max_ratio = sequence_with_ratio.get(seq)
            max_ratio_seq = seq

    return max_ratio_seq

def calc_project(project_id):
    match_details = pandas.read_csv("%s/%s_spec_match_details.csv"%(project_id, project_id))
    match_details_filtered = match_details.loc[(match_details['f_val'] >= 0.5) & (match_details['cluster_size'] >= 10)]
    clusters = get_cluster_data(match_details_filtered)

    spec_match_own_cluster = {}
    spec_match_other_cluster = {}
    spec_match_new_cluster = {}

    for index, row in match_details_filtered.iterrows():
        spec_title = row.get('spec_title')
        matched_cluster_id = row.get('cluster_id')
        spectra = clusters.get(matched_cluster_id).get('spectra_titles')
        if spec_title in spectra:
            spec_match_own_cluster[spec_title] = matched_cluster_id
        else:
            origin_cluster_id = get_spec_in_which_cluster(spec_title, project_id)
            if origin_cluster_id == '' or origin_cluster_id == None:
                spec_match_new_cluster[spec_title] = matched_cluster_id
            else:
                spec_match_other_cluster[spec_title] = {'origin_cluster': origin_cluster_id,
                                                        'matched_cluster': matched_cluster_id}

    spec_match_other_sequence = {}
    for spec in spec_match_other_cluster.keys():
        info = spec_match_other_cluster.get(spec)
        origin_peptide = get_origin_peptide(spec, project_id).replace("I", "L")
        origin_cluster_id = info.get('origin_cluster')
        matched_cluster_id = info.get('matched_cluster')
        main_peptide_in_matched_cluster = get_main_peptide(clusters, matched_cluster_id).replace("I", "L")
        if origin_peptide != main_peptide_in_matched_cluster:
            spec_match_other_sequence[spec] = \
                {'origin_cluster': origin_cluster_id, 'matched_cluster': matched_cluster_id, \
                 'origin_peptide': origin_peptide, 'matched_peptide': main_peptide_in_matched_cluster
                 }
    print("Project: %s"%(project_id))
    print("spec_match_to: own_cluster: %d, new_cluster: %d, other_cluster: %d(other_seq: %d)"% \
              (len(spec_match_own_cluster) , len(spec_match_new_cluster), len(spec_match_other_cluster), len(spec_match_other_sequence)))

    print("Error_rate: %f%%"%(100 * len(spec_match_other_sequence) / (
                len(spec_match_own_cluster) + len(spec_match_other_cluster) - len(spec_match_other_sequence))))

def main():
    arguments = docopt(__doc__, version='analysis_pipeline.py 1.0 BETA')
    project_id = arguments['--project'] or arguments['-p']
    calc_project(project_id)
    return

if __name__ == "__main__":
    main()
