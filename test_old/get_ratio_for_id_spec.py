"""
This program get the fainal statitical analysis on the library searched result.
"""

import pymysql.cursors
import sys, os
import numpy as np
import pandas as pd
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import retrieve_splib_result as retriever
import identi_data_to_file as id_retriever

def get_connection(mysql_host):
    #build the connection
    connection = pymysql.connect(host=mysql_host,
                            user='pride_cluster_t',
                            password='pride123',
                            db='pride_cluster_t',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
    print("Opened database successfully");
    #deal with the table name
    return connection

def get_spec_lib_match(connection,project):
    """
    Not proper to use
    """
    identi_table = project 
    lib_search_table = project + "_lib_result"
    fval_threshold = "0.5"
    spec_matchs = dict()

    select_str = "SELECT b.spec_title, b.scan_in_lib FROM %s a, %s b WHERE b.fval >= %s AND a.spectra_title = b.spec_title"% \
                 (identi_table, lib_search_table, fval_threshold)
    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()
    for result in results:
        spec_title = result.get('spec_title')
        scan_num =  result.get('scan_in_lib')
        spec_matchs[spec_title] = scan_num
    return(spec_matchs)

def print_intersection(identified_n, intersection_n, lib_match_n):
    identified_in = 100*intersection_n/identified_n
    identified_out = 100 - identified_in
    lib_match_in = 100*intersection_n/lib_match_n
    lib_match_out = 100 - lib_match_in
    print("Got %d identified_spectra"%(identified_n))
    print("Got %d intersection spectra"%(intersection_n))
    print("Got %d likely good matched spectra"%(lib_match_n))
    print("We can identify %.1f %% more spectra than search engin"%(100*(lib_match_n - intersection_n)/identified_n))
    print("[ %d   [ %d ]  %d ]"%(identified_n - intersection_n, intersection_n, lib_match_n - intersection_n))
    print("[ %.1f   [ %.1f | %.1f ]  %.1f ]"%(identified_out, identified_in, lib_match_in, lib_match_out))


def get_spec_lib_match_from_file():
    """
    Only get identified and library-searching matched spectra, and their scan number in the original mzML file
    The default fval threashold is 0.5
    """
    fval_threshold = 0.5
    both_matchs = dict()
    unid_matchs = dict()
    
    identified_spectra = set()
    lib_search_matched_no = 0
    with open('./identified_spectra.tab','r') as identified_file, open('lib_search_result.tab','r') as result_file:
        line = identified_file.readline()
        for line in identified_file.readlines():
            spec_title = line.split('\t')[0]
            identified_spectra.add(spec_title)
        
        line = result_file.readline()
        total_matchs_n = 0
        for line in result_file.readlines():
            words = line.split('\t')
            spec_title = words[0]
            fval = float(words[3])
            if fval >= fval_threshold:
                total_matchs_n += 1
                scan_n= words[1]
                if spec_title in identified_spectra:
                    both_matchs[spec_title] = scan_n
                else:
                    unid_matchs[spec_title] = scan_n

    print_intersection(len(identified_spectra), len(both_matchs), total_matchs_n)
    """
    identified_in = 100*len(both_matchs)/len(identified_spectra)
    identified_out = 100 - identified_in
    lib_match_in = 100*len(both_matchs)/total_matchs
    lib_match_out = 100 - lib_match_in
    print("Got %d identified_spectra"%(len(identified_spectra)))
    print("Got %d intersection spectra"%(len(both_matchs)))
    print("Got %d likely good matched spectra"%(total_matchs))
    print("We can identify %f %% more spectra than search engin"%(100*(total_matchs - len(both_matchs))/len(identified_spectra)))
    print("[ %d   [ %d ]  %d ]"%(len(identified_spectra)-len(both_matchs), len(both_matchs), total_matchs-len(both_matchs)))
    print("[ %.1f   [ %.1f | %.1f ]  %.1f ]"%(identified_out, identified_in, lib_match_in, lib_match_out))
    """
    return(len(identified_spectra), both_matchs, unid_matchs)

def get_scannum_cluster_map(connection):
    scan_map_table = "test_spec_lib_index"
    select_str = "SELECT * FROM %s "% \
                 (scan_map_table)
    scan_id_pairs = dict()

    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()
    for result in results:
        scan_num = result.get('scan_num')
        cluster_id = result.get('cluster_id')
        scan_id_pairs[scan_num] = cluster_id

def write_scannum_cluster_map_to_file(connection):
    scan_map_file = "/home/ubuntu/mingze/spec_lib_searching/phospho/library_scan_map.tab"
    scan_id_pairs = dict()

    scan_map_table = "test_spec_lib_index"
    select_str = "SELECT * FROM %s "% \
                 (scan_map_table)
    scan_id_pairs = dict()

    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()
    with open(scan_map_file, 'w') as o:
        o.write("%s\t%s\n"%('scan_num', 'cluster_id'))
        for result in results:
            scan_num = result.get('scan_num')
            cluster_id = result.get('cluster_id')
            o.write("%s\t%s\n"%(scan_num, cluster_id))

def get_scannum_cluster_map_from_file():
    scan_map_file = "/home/ubuntu/mingze/spec_lib_searching/phospho/library_scan_map.tab"
    scan_id_pairs = dict()

    with open(scan_map_file, 'r') as o:
        line = o.readline()
        for line in o.readlines():
            words = line.strip().split('\t')
            scan_num = words[0]
            cluster_id = words[1]
            scan_id_pairs[scan_num] = cluster_id
    return(scan_id_pairs)
    
def get_qualified_clusters(connection):
    cluster_table = "201504_3"
#    ratio_threshold = "0.618"
    clusters = dict()

    select_str = "SELECT cluster_id,cluster_ratio FROM %s "% \
                 (cluster_table)
    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()
    for result in results:
        cluster_id = result.get('cluster_id')
        clusters[cluster_id] = result.get('cluster_ratio')
    return(clusters)

def write_qualified_clusters(connection):
    cluster_table = "201504_3"
#    ratio_threshold = "0.618"
    cluster_list_file = "/home/ubuntu/mingze/spec_lib_searching/phospho/cluster_list.tab"
    clusters = dict()

    select_str = "SELECT cluster_id,cluster_ratio, n_spec FROM %s "% \
                 (cluster_table)
    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()
    with open(cluster_list_file, 'w') as o:
        o.write("%s\t%s\t%s\n"%('cluster_id','n_spec', 'ratio'))
        for result in results:
            cluster_id = result.get('cluster_id')
            n_spec = result.get('n_spec')
            ratio = result.get('cluster_ratio')
            o.write("%s\t%s\t%s\n"%(cluster_id, n_spec, ratio))

def get_qualified_clusters_from_file():
#    ratio_threshold = 0.618
    size_threshold = 5
    cluster_list_file = "/home/ubuntu/mingze/spec_lib_searching/phospho/cluster_list_%d.tab"%(size_threshold)
    
    cluster_in_list = list()
    with open(cluster_list_file, 'r') as o:
        line = o.readline()
        for line in o.readlines():
            temp_list = list()
            words = line.strip().split('\t')
            temp_list.append(words[0])
            temp_list.append(words[1]) 
            temp_list.append(words[2])
            cluster_in_list.append(temp_list)
    clusters = pd.DataFrame(cluster_in_list, columns=('cluster_id', 'n_spec', 'ratio'))
    return(clusters)

def get_final(spectra_match_list, scannum_cluster_map, qualified_clusters):
    final_list = list()
    cluster_ids = qualified_clusters.keys()
    for spectrum in spectra_match_list.keys():
        scan = spectra_match_list.get(spectrum)
        matched_cluster = scannum_cluster_map.get(scan)        
        if matched_cluster not in cluster_ids:
            continue
        print("%s\t%s\t%s"%(spectrum, matched_cluster, qualified_clusters[matched_cluster]))

    return(final_list)

def final_to_file(spectra_match_list, unid_match_list, scannum_cluster_map, qualified_clusters):
    id_final_result_file = 'final_result_id.tab'
    unid_final_result_file = 'final_result_unid.tab'
    indexed_clusters = qualified_clusters.set_index('cluster_id')
    qualified_cluster_ids = set(qualified_clusters['cluster_id'].tolist())
    id_final_spectra_n = 0
    with open(id_final_result_file, 'w') as o:
        o.write("%s\t%s\t%s\t%s\n"%('spec_title','matched_cluster_id', 'cluster_ratio','cluster_size'))
        for spectrum in spectra_match_list.keys():
            scan = spectra_match_list.get(spectrum)
            matched_cluster = scannum_cluster_map.get(scan)        
            if matched_cluster not in qualified_cluster_ids:
                continue
            cluster_info = indexed_clusters.loc[matched_cluster]
            ratio = cluster_info['ratio']
            n_spec = cluster_info['n_spec']
            o.write("%s\t%s\t%s\t%s\n"%(spectrum, matched_cluster, ratio, n_spec))
            id_final_spectra_n += 1

    unid_final_spectra_n = 0
    with open(unid_final_result_file, 'w') as o:
        o.write("%s\t%s\t%s\t%s\n"%('spec_title','matched_cluster_id', 'cluster_ratio','cluster_size'))
        for spectrum in unid_match_list.keys():
            scan = spectra_match_list.get(spectrum)
            matched_cluster = scannum_cluster_map.get(scan)        
            if matched_cluster not in qualified_cluster_ids:
                continue
            cluster_info = indexed_clusters.loc[matched_cluster]
            ratio = cluster_info['ratio']
            n_spec = cluster_info['n_spec']
            o.write("%s\t%s\t%s\t%s\n"%(spectrum, matched_cluster, n_spec, ratio))
            unid_final_spectra_n += 1
    return(id_final_spectra_n, unid_final_spectra_n)

def prepare_shared_files():
    connection = get_connection("localhost")
    write_scannum_cluster_map_to_file(connection)
    write_qualified_clusters(connection)

def prepare_project_files():
    input_path = './'
    libmatch_output_file = './lib_search_result.tab'
    id_output_file = './identified_spectra.tab'
    retriever.process(input_path, libmatch_output_file)
#    id_retriever.process(input_path, id_output_file) 
    
def main():
    print("Start to calculate project: " + os.path.abspath('.'))
#    prepare_shared_files()
    prepare_project_files()
    return
#get all library matched and identified spectra 
    (identified_n, spectra_match_list, unid_match_list) = get_spec_lib_match_from_file()
    
    #get map from scan_num to cluster_id 
#    scannum_cluster_map = get_scannum_cluster_map(connection)
    scannum_cluster_map = get_scannum_cluster_map_from_file()
    #get all clusters whose size >=5
    qualified_clusters = get_qualified_clusters_from_file()

    #get intersection from two lists
    (id_final_spectra_n, unid_final_spectra_n) = final_to_file(spectra_match_list, unid_match_list, scannum_cluster_map, qualified_clusters)    
    print_intersection(identified_n, id_final_spectra_n, id_final_spectra_n + unid_final_spectra_n)

    #get_final_results
if __name__ == "__main__":
    main()
