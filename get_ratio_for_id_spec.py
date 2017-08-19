"""
This program get the ratios of the clusters, which are connected to the identified spectra by SpectraST searching.
"""

import pymysql.cursors

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

def get_spec_lib_match(connection):
    identi_table = "test_spec_lib1_identi"
    lib_search_table = "test_spec_lib_search_result_1"
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
    return(scan_id_pairs)

def get_high_ratio_clusters(connection):
    cluster_table = "201504_3"
    ratio_threshold = "0.618"
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

def get_final(spectra_match_list, scannum_cluster_map, high_ratio_clusters):
    final_list = list()
    cluster_ids = high_ratio_clusters.keys()
    for spectrum in spectra_match_list.keys():
        scan = spectra_match_list.get(spectrum)
        matched_cluster = scannum_cluster_map.get(scan)        
        if matched_cluster not in cluster_ids:
            continue
        print("%s\t%s\t%s"%(spectrum, matched_cluster, high_ratio_clusters[matched_cluster]))


    return(final_list)

def main():
    connection = get_connection("localhost")

    #get all library matched and identified spectra 
    spectra_match_list = get_spec_lib_match(connection)
    
    #get map from scan_num to cluster_id 
    scannum_cluster_map = get_scannum_cluster_map(connection)
    #get all clusters whose ratio >0.618
    high_ratio_clusters = get_high_ratio_clusters(connection)

    #get intersection from two lists
    final_spectra_list = get_final(spectra_match_list, scannum_cluster_map, high_ratio_clusters)    
#    print(final_spectra_list)
    return

    #get_final_results
if __name__ == "__main__":
    main()
