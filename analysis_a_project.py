#!/usr/bin/python3
"""
This program get the fainal statitical analysis on the library searched result of a project.
"""

import pymysql.cursors
import sys, os
import pandas as pd
import logging 
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import retrieve_splib_result as retriever
#import identi_data_to_file as id_retriever
import phoenix_import_util as phoenix 

def get_connection(mysql_host):
    #build the connection to mysql database, deprected
    connection = pymysql.connect(host=mysql_host,
                            user='pride_cluster_t',
                            password='pride123',
                            db='pride_cluster_t',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
    print("Opened database successfully");
    #deal with the table name
    return connection

def print_intersection(project_id, identified_n, intersection_n, lib_match_n):
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
    print("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"% \
            ('project_id', 'id_out_n', 'share_n', 'lib_out_n', 'id_out_p', 'id_share_p', 'lib_share_p','lib_out_p'))
    print("%s\t%d\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f"% \
            (project_id, identified_n - intersection_n, intersection_n, lib_match_n - intersection_n, identified_out, identified_in, lib_match_in, lib_match_out))


def get_spec_lib_match_from_file(project_id):
    """
    Only get identified  and unidentified library-searching matched spectra in different list.
    The default fval threashold is 0.5
    """
    fval_threshold = 0.5
    id_matchs = dict()
    unid_matchs = dict()
    
    identified_spectra = set()
    lib_search_matched_no = 0
    
    if not os.path.exists (project_id + '/identified_spectra.tab'):
        print("error, no file " + project_id + '/identified_spectra.tab')
        sys.exit(1)

    with open(project_id + '/identified_spectra.tab','r') as identified_file, open(project_id + '/lib_search_result.tab','r') as result_file:
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
                lib_spec_id= words[1]
                if spec_title in identified_spectra:
                    id_matchs[spec_title] = lib_spec_id
                else:
                    unid_matchs[spec_title] = lib_spec_id

    print_intersection(project_id, len(identified_spectra), len(id_matchs), total_matchs_n)
    """
    identified_in = 100*len(id_matchs)/len(identified_spectra)
    identified_out = 100 - identified_in
    lib_match_in = 100*len(id_matchs)/total_matchs
    lib_match_out = 100 - lib_match_in
    print("Got %d identified_spectra"%(len(identified_spectra)))
    print("Got %d intersection spectra"%(len(id_matchs)))
    print("Got %d likely good matched spectra"%(total_matchs))
    print("We can identify %f %% more spectra than search engin"%(100*(total_matchs - len(id_matchs))/len(identified_spectra)))
    print("[ %d   [ %d ]  %d ]"%(len(identified_spectra)-len(id_matchs), len(id_matchs), total_matchs-len(id_matchs)))
    print("[ %.1f   [ %.1f | %.1f ]  %.1f ]"%(identified_out, identified_in, lib_match_in, lib_match_out))
    """
    return(len(identified_spectra), id_matchs, unid_matchs)


"""
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
"""

"""
this function get clusters from mysql database based on some restrictions(cluster ratio/size)
and write it into files for further using.
"""
def write_qualified_clusters(connection):
    cluster_table = "201504_3"
#    ratio_threshold = "0.618"
    size_threshold = 5
    cluster_list_file = "/home/ubuntu/mingze/spec_lib_searching/phospho/cluster_list_%d.tab"%(size_threshold)
    if os.path.isfile(cluster_list_file) and os.path.getsize(cluster_list_file)>10000:
        print("cluster_list_%d.tab is already there."%(size_threshold))
        return
    clusters = dict()

    select_str = "SELECT cluster_id,cluster_ratio, n_spec FROM %s WHERE n_spec>=%d"% \
                 (cluster_table, size_threshold)
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

"""
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
"""
def write_result_to_file(project_id, id_match_list, unid_match_list, qualified_clusters):
    id_result_file = project_id + '/result_id.tab'
    unid_result_file = project_id + '/result_unid.tab'
    indexed_clusters = qualified_clusters.set_index('cluster_id')
    qualified_cluster_ids = set(qualified_clusters['cluster_id'].tolist())
    if os.path.isfile(id_result_file) and os.path.getsize(id_result_file)>1000:
        #print("%s is already there."%(id_result_file))
        pass
    else:
        with open(id_result_file, 'w') as o:
            o.write("%s\t%s\t%s\t%s\n"%('spec_title','matched_cluster_id', 'cluster_ratio','cluster_size'))
            for spectrum in id_match_list.keys():
                matched_cluster = id_match_list.get(spectrum)
                cluster_info = indexed_clusters.loc[matched_cluster]
                ratio = cluster_info['ratio']
                n_spec = cluster_info['n_spec']
                o.write("%s\t%s\t%s\t%s\n"%(spectrum, matched_cluster, ratio, n_spec))
    
    if os.path.isfile(unid_result_file) and os.path.getsize(unid_result_file)>1000:
        #print("%s is already there."%(unid_result_file))
        pass
    else:
        with open(unid_result_file, 'w') as o:
            o.write("%s\t%s\t%s\t%s\n"%('spec_title','matched_cluster_id', 'cluster_ratio','cluster_size'))
            for spectrum in unid_match_list.keys():
                matched_cluster = unid_match_list.get(spectrum)
                cluster_info = indexed_clusters.loc[matched_cluster]
                ratio = cluster_info['ratio']
                n_spec = cluster_info['n_spec']
                o.write("%s\t%s\t%s\t%s\n"%(spectrum, matched_cluster, ratio, n_spec))



"""
prepare the files which will be used by all projects, e.g. the clusters file 
"""
def prepare_shared_files():
    connection = get_connection("localhost")
#    write_scannum_cluster_map_to_file(connection)
    write_qualified_clusters(connection)


"""
prepare the lib_search_result.tab and identified_spectra.tab file for each project,
for the next step analysis
"""
def prepare_project_files(project_id):
    input_path = project_id + '/'
    libmatch_output_file = project_id + '/lib_search_result.tab'
    id_output_file = project_id + '/identified_spectra.tab'

    if os.path.isfile(id_output_file) and os.path.getsize(id_output_file)>1000:
        print("%s is already there."%(id_output_file))
    else:
        #id_retriever.process(input_path, id_output_file)  #removed because the idenfications are nolonger in mgf files, but from pride xml files
        phoenix.retrieve_identification_from_phoenix(project_id, "localhost", id_output_file)

    retriever.process(project_id, input_path, libmatch_output_file)

    
def main():
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    logging.info('Started')
    project_id = sys.argv[1]
    if project_id == None:
        raise Exception("No project id inputed, failed to do the analysis.")
    print("Start to calculate project: " + project_id)

    prepare_shared_files()
    prepare_project_files(project_id)
    #get all library matched and identified spectra 
    (identified_n, id_match_list, unid_match_list) = get_spec_lib_match_from_file(project_id)
    
    #get all clusters whose size >=5
    qualified_clusters = get_qualified_clusters_from_file()

    #write results and get intersection from two lists
    write_result_to_file(project_id, id_match_list, unid_match_list, qualified_clusters)    
#    print_intersection(identified_n, id_final_spectra_n, id_final_spectra_n + unid_final_spectra_n)
    logging.info('Finished')

if __name__ == "__main__":
    main()
