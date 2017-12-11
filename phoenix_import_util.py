import phoenixdb
import time
import math
import os,sys,re,json

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import confident_score_calc as cf_calc 

cluster_table = "V_CLUSTER"
lib_spec_table = cluster_table + "_SPEC"

"""
Get connection 
"""
def get_conn(host):
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    return conn

"""
Get sequences' ratio from cluster, when the sequence is not matched to the max_seq
If no seq matched, return 0.0
"""
def get_seq_ratio(spectrum_pep, cluster_id, conn):
    spectrum_pep = spectrum_pep.replace("I", "L")
    spectrum_pep_list = spectrum_pep.split("||")

    sql_str = "SELECT SPEC_TITLE, ID_SEQUENCES, SEQ_RATIO FROM " + lib_spec_table + \
        "WHERE CLUSTER_FK = " + cluster_id
    this_seq_ratio = 0.0
    with conn.cursor() as cursor :
        cursor.execute(sql_str)
        rs = cursor.fetchall
        for r in rs:
            spec_title = r[0]
            id_seqs = r[1]
            seq_ratio = r[2]
            id_seq_list = id_seqs.split("||")
            for id_seq in id_seq_list:
                for spec_pep_seq in spectrum_pep_list:
                    if id_seq == spec_pep_seq:
                        this_seq_ratio = seq_ratio
                        return this_seq_ratio
    return this_seq_ratio


"""
Get spectra identify data from identification table
"""       
def get_spectra_pep(prj_id, host):
    id_table = "T_" + prj_id.upper() + "_PSM"
    sql_str = "SELECT SPECTRUM_TITLE, PEPTIDE_SEQUENCE, MODIFICATIONS FROM " + id_table + ""
    conn = get_conn(host)
    cursor = conn.cursor()
    cursor.execute(sql_str)
    spectra_rs = cursor.fetchall()
    spectra_peps = {}
    for r in spectra_rs:
        spectra_title = r[0]
        pep_seq = r[1]
        mod_seq = r[2]
        spectra_peps[spectra_title] = {"seq":pep_seq, "mods":mod_seq}
    print("got %d identified spectra"%(len(spectra_peps)))
    return spectra_peps
    cursor.close()
    conn.close()

"""
Read cluster data from phoenix tables
"""
def get_cluster_data(search_results, host):
    cluster_data = dict()
    conn = get_conn(host)
    cursor = conn.cursor()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        cluster_id = search_result.get('lib_spec_id')
        cluster_query_sql = "SELECT CLUSTER_RATIO, N_SPEC FROM " + cluster_table + " WHERE CLUSTER_ID = '" + cluster_id + "'"
        # print(cluster_query_sql)
        cursor.execute(cluster_query_sql)
        result = cursor.fetchone()
        cluster = dict()
        cluster['ratio'] = result[0]
        cluster['size'] = result[1]
        cluster_data[cluster_id] = cluster
    cursor.close() 
    conn.close()
    return cluster_data

"""
Export search result of a project to phoenix/hbase table
"""
def export_sr_to_phoenix(project_id, host, search_results):

    conn = get_conn(host)
    cursor = conn.cursor()
    
    match_table_name = "T_" + project_id  + "spec_cluster_match" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + match_table_name.upper() + "\" (" + \
             "spec_title VARCHAR NOT NULL PRIMARY KEY ," + \
             "dot FLOAT ,"   + \
             "f_val FLOAT, "  + \
             "conf_sc FLOAT, "  + \
             "cluster_id VARCHAR "   + \
             ")"
    cursor.execute(create_table_sql)
   
    #row = [cluster_id, pep_seq, conf_score, f_val, cluster_ratio, cluster_size, recommend_pep_seq, num_spec, spectra]

    spec_peps = get_spectra_pep(project_id, host)
    conf_sc_set = cf_calc.calculate_conf_sc(search_results, spec_peps, host)
    print(str(conf_sc_set))
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        dot = float(search_result.get('dot'))
        fval = float(search_result.get('fval'))
        conf_sc = float(conf_sc_set[spec_title]['conf_score'])
        cluster_id = search_result.get('lib_spec_id')
        upsert_sql = "UPSERT INTO " + match_table_name + " VALUES ('%s', %f, %f, %f, '%s')"%(spec_title, dot, fval, conf_sc, cluster_id)
        # print(upsert_sql)

        cursor.execute(upsert_sql)

    cluster_data = get_cluster_data(search_results, host)
    upsert_scored_psm_table(project_id, search_results,conf_sc_set, cluster_data, host)
        
    cursor.close()
    conn.close()

def upsert_scored_psm_table(project_id, search_results, conf_sc_set, cluster_data, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    spectra_matched_to_cluster = dict()
    unid_spec_matched_to_cluster = dict() #cluster_id as the key
    identified_spectra = retrieve_identification_from_phoenix(project_id, "localhost", None)
    scored_psm_table_name = "T_" + project_id  + "_scored_psm_" + time.strftime("%Y%m%d")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + scored_psm_table_name.upper() + "\" (" + \
                       "id INTEGER NOT NULL PRIMARY KEY," + \
                       "cluster_id VARCHAR, "   + \
                       "pep_seq VARCHAR, "   + \
                       "conf_sc FLOAT, "  + \
                       "f_val FLOAT, "  + \
                       "cluster_ratio FLOAT, "   + \
                       "cluster_size INTEGER, "   + \
                       "recommend_pep VARCHAR, " + \
                       "recommend_mods VARCHAR, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR " + \
                       ")"
    cursor.execute(create_table_sql)

    recom_pep_table_name = "T_" + project_id  + "_recomm_id_" + time.strftime("%Y%m%d")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + recom_pep_table_name.upper() + "\" (" + \
                       "id INTEGER NOT NULL PRIMARY KEY," + \
                       "cluster_id VARCHAR, "   + \
                       "conf_sc FLOAT, "  + \
                       "f_val FLOAT, "  + \
                       "cluster_ratio FLOAT, "   + \
                       "cluster_size INTEGER, "   + \
                       "recommend_pep VARCHAR, " + \
                       "recommend_mods VARCHAR, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR " + \
                       ")"
    cursor.execute(create_table_sql)

    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        cluster_id = search_result.get('lib_spec_id')
        matched_spectra = spectra_matched_to_cluster.get(cluster_id,[]) 
        matched_spectra.append(spec_title)
        spectra_matched_to_cluster[cluster_id] = matched_spectra
    id_row_num = 1
    unid_row_num = 1
    for cluster_id in spectra_matched_to_cluster.keys():
        matched_spectra = spectra_matched_to_cluster.get(cluster_id,[])
        matched_peptides = dict()
        for matched_spec in matched_spectra:
            pep_seq = identified_spectra.get(matched_spec,None)
            if pep_seq:
                pep_spectra = matched_peptides.get(pep_seq,[])
                pep_spectra.append(matched_spec)
                matched_peptides[pep_seq] = pep_spectra
            else:
                matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id,[])
                matched_unid_spec.append(matched_spec)
                unid_spec_matched_to_cluster[cluster_id] = matched_unid_spec
        # print("got %d pep_seq for cluster %s"%(len(matched_peptides), cluster_id))
        for pep_seq in matched_peptides.keys():
            pep_spectra = matched_peptides.get(pep_seq,[])
            spec1 = pep_spectra[0] #get the first spec in list
            conf_score = conf_sc_set.get(spec1).get('conf_score')
            f_val = float(search_results.get(spec1).get('fval'))
            num_spec = len(pep_spectra)
            spectra = "||".join(pep_spectra)
            recommend_pep_seq = conf_sc_set.get(spec1).get('recommend_pep_seq')
            recommend_mods = conf_sc_set.get(spec1).get('recommend_mods')
            cluster = cluster_data.get(cluster_id)
            cluster_ratio = cluster.get('ratio')
            cluster_size = cluster.get('size')
            
            upsert_sql = "UPSERT INTO " + scored_psm_table_name.upper() + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(upsert_sql, ( \
                id_row_num, cluster_id, pep_seq, conf_score, f_val, \
                cluster_ratio, cluster_size, recommend_pep_seq, recommend_mods, num_spec, spectra \
            ))
            # print("upsert pep seq %s in cluster %s, with spectra:%s"%(pep_seq, cluster_id, spectra))
            id_row_num += 1

        matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id)
        if matched_unid_spec !=None and len(matched_unid_spec) > 0:
            spec1 = matched_unid_spec[0] #get the first spec in list
            conf_score = conf_sc_set.get(spec1).get('conf_score')
            f_val = float(search_results.get(spec1).get('fval'))
            num_spec = len(matched_unid_spec )
            spectra = "||".join(matched_unid_spec )
            recommend_pep_seq = conf_sc_set.get(spec1).get('recommend_pep_seq')
            recommend_mods = conf_sc_set.get(spec1).get('recommend_mods')
            cluster = cluster_data.get(cluster_id)
            cluster_ratio = cluster.get('ratio')
            cluster_size = cluster.get('size')
            upsert_sql = "UPSERT INTO " + recom_pep_table_name.upper() + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(upsert_sql, ( \
                unid_row_num, cluster_id, conf_score, f_val, \
                cluster_ratio, cluster_size, recommend_pep_seq, recommend_mods, num_spec, spectra \
                ))
            unid_row_num += 1
    cursor.close()
    conn.close()
    
"""
Get all matched cluster results from phoenix 
"""
def get_lib_rs_from_phoenix(search_results, host):
    lib_result = dict()
    conn = get_conn(host) 
    cursor = conn.cursor()
    clusters = dict()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        cluster_id = search_result.get('lib_spec_id')
        sql_str = "SELECT CLUSTER_RATIO, N_SPEC, N_ID_SPEC, N_UNID_SPEC, SEQUENCES_RATIOS, SEQUENCES_MODS from \"" + cluster_table +"\" WHERE CLUSTER_ID = '" + cluster_id + "'"
        cursor.execute(sql_str)
        rs = cursor.fetchone()
        if rs == None:
            continue
        cluster = {}
        cluster['ratio'] = rs[0]
        cluster['n_spec'] = rs[1]
        cluster['seqs_ratios'] = rs[4]
        cluster['seqs_mods'] = rs[5]
        clusters[cluster_id] = cluster
    cursor.close()
    conn.close()
    if len(clusters) < 1:
        raise Exception("Got empty cluster set for this search result")
    return(clusters)
        
"""
Export identification result of a project to phoenix/hbase table
"""
def export_ident_to_phoenix(project_id, host, identifications):
    """
        o.write("spectrum_title\tsequence\n")
        for spec_title in identifications.keys():
            o.write("%s\t%s\n"%(spec_title, identifications[spec_title]))
    """

    start = time.time()
    print("start phoenix inserting at " + str(start))
    table_name = "t_identifications_" + project_id + "_" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + table_name.upper() + "\" (" + \
             "spec_title VARCHAR NOT NULL PRIMARY KEY , " + \
             "pep_seq VARCHAR "   + \
             ")"
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    for spec_title in identifications.keys():
        upsert_sql = "UPSERT INTO " + table_name + " VALUES (?, ?)"
        cursor.execute(upsert_sql, (spec_title,identifications[spec_title]))

    cursor.execute("SELECT count(*) FROM " + table_name)
    print(cursor.fetchone())

    cursor.close()
    conn.close()

    end = time.time()
    print("end phoenix inserting at " + str(end))
    print("totally time " + str(end - start))


def import_clusters_to_phoenix(connection):
    old_cluster_table = "201504_3"
#    ratio_threshold = "0.618"
    size_threshold = 2
    clusters = dict()

    select_str = "SELECT cluster_id,cluster_ratio, n_spec FROM %s WHERE n_spec>=%d"% \
                 (old_cluster_table, size_threshold)
    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()

    start = time.time()
    print("start phoenix inserting at " + str(start))
    table_name = "identifications_" + project_id + "_" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + table_name.upper() + "\" (" + \
             "spec_title VARCHAR NOT NULL PRIMARY KEY , " + \
             "pep_seq VARCHAR "   + \
             ")"
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    """
    with open(cluster_list_file, 'w') as o:
        o.write("%s\t%s\t%s\n"%('cluster_id','n_spec', 'ratio'))
        for result in results:
            cluster_id = result.get('cluster_id')
            n_spec = result.get('n_spec')
            ratio = result.get('cluster_ratio')
            o.write("%s\t%s\t%s\n"%(cluster_id, n_spec, ratio))
    """

    cursor.execute("SELECT count(*) FROM " + table_name)
    print(cursor.fetchone())

    cursor.close()
    conn.close()

    end = time.time()
    print("end phoenix inserting at " + str(end))
    print("totally time " + str(end - start))

def retrieve_identification_from_phoenix(project_id, host, output_file):
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()
    table_name = "T_" + project_id.upper() + "_PSM"
    sql_str = "SELECT count(*) FROM " + table_name + "" ;
    cursor.execute(sql_str)
    r = cursor.fetchone()
    total_n = r[0]
    
    psms = dict()    
    offset = 0 
    while(offset < total_n):
        sql_str = "SELECT SPECTRUM_TITLE,PEPTIDE_SEQUENCE FROM " + table_name + " LIMIT 5000 OFFSET " + str(offset)
        cursor.execute(sql_str)
        rs = cursor.fetchall()
        for r in rs:
            spec_title = r[0]
            id_seq = r[1]
            psms[spec_title] = id_seq
        offset += 5000

    cursor.close()
    conn.close()

    if output_file != None:
        with open(output_file,"w") as o:
            o.write("spectrum_title\tsequence\n")
            for spec in psms.keys():
                o.write(spec + "\t" + psms.get(spec)+ "\n")
    return psms


def test_cluster_select():
    host = "localhost"
    database_url = 'http://' + host + ':8765/'
    cluster_table = "V_CLUSTER"
    cluster_id = "14b08180-051a-4dd7-8087-6095db2704b2"
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor()
    cluster_query_sql = "SELECT CLUSTER_RATIO, N_SPEC FROM \"" + cluster_table + "\" WHERE CLUSTER_ID = '" + cluster_id + "'"
    # print(cluster_query_sql)
    cursor.execute(cluster_query_sql)
    result = cursor.fetchone()
    print( result[0])
    print( result[1])


#retrieve_identification_from_phoenix("pxd000021", "localhost", "output.txt")
test_cluster_select()
"""
project_id = 'test00002'
host = 'localhost'
search_results = {} 
identifications = {} 

result1 = {'lib_spec_id':"000001f5-cb21-4db9-9ece-6ea8ab3a7ded", 'dot':"0.1", 'fval':"0.1"}
result2 = {'lib_spec_id':"0000047f-5bdf-4302-9c1f-8fe7fa0f2971", 'dot':"0.1", 'fval':"0.1"}
result3 = {'lib_spec_id':"00000839-e89d-4e99-ac51-1f370173bf8f", 'dot':"0.1", 'fval':"0.1"}
search_results['title1'] = result1
search_results['title2'] = result1
search_results['title3'] = result1
identifications['title1'] = "squence1"
identifications['title2'] = "squence2"
identifications['title3'] = "squence3"



database_url = 'http://localhost:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
#get_lib_rs_from_phoenix(search_results, conn)
#get_spectra("PXD000021", conn)
export_sr_to_phoenix(project_id, host, search_results)
export_ident_to_phoenix(project_id, host, identifications)
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
cursor.execute("UPSERT INTO users VALUES (?, ?)", (2, 'user1'))
cursor.execute("UPSERT INTO users VALUES (?, ?)", (3, 'user2'))
cursor.execute("SELECT * FROM users")
print(cursor.fetchall())
"""



