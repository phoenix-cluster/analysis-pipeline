import pymysql
import time
import os, sys, json
import logging

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

cluster_table = "V_CLUSTER"
lib_spec_table = cluster_table + "_SPEC"

"""
Get connection 
"""
def get_conn(host):
    conn = pymysql.connect(host = host, port=3309,
                           user="phoenix_enhancer",
                           passwd="enhancer123",
                           db="phoenix_enhancer",
                           autocommit=True,
                           local_infile=1)
    return conn


"""
Read cluster data from mysql db tables
"""
def get_cluster_data(search_results, host):
    cluster_data = dict()
    conn = get_conn(host)
    cursor = conn.cursor()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        cluster_id = search_result.get('lib_spec_id')
        cluster_query_sql = "SELECT CLUSTER_RATIO, N_ID_SPEC, CONF_SC, SEQUENCES_RATIOS FROM " + cluster_table + " WHERE CLUSTER_ID = '" + cluster_id + "'"
        # print(cluster_query_sql)
        cursor.execute(cluster_query_sql)
        result = cursor.fetchone()
        cluster = dict()
        cluster['ratio'] = result[0]
        cluster['size'] = result[1]
        cluster['conf_sc'] = result[2]
        cluster['seqs_ratios'] = result[3]
        cluster_data[cluster_id] = cluster
    cursor.close()
    conn.close()
    return cluster_data


"""
Read cluster data from mysql db tables
"""
def get_all_clusters(host, cluster_table_name, min_size):
    cluster_data = list()
    conn = get_conn(host)
    cursor = conn.cursor()
    cluster_query_sql = "SELECT CLUSTER_ID, CLUSTER_RATIO, N_ID_SPEC, SEQUENCES_RATIOS, CONF_SC, SEQUENCES_MODS FROM " + cluster_table_name
    cluster_query_sql += " where N_ID_SPEC >= " + str(min_size)
    cursor.execute(cluster_query_sql)
    rs = cursor.fetchall()
    for r in rs:
        if r == None:
            continue
        cluster = dict()
        cluster['id'] = r[0]
        cluster['ratio'] = r[1]
        cluster['size'] = r[2]
        cluster['seqs_ratios'] = r[3]
        cluster['conf_sc'] = r[4]
        cluster['seqs_mods'] = r[5]
        # if r[1] == None or r[2] == None or r[3] == None or r[4] == None or r[5] == None:
        #     print("cluster " + r[0] + "has none field: " + str(cluster))
        #     continue
        cluster_data.append(cluster)
    cursor.close()
    conn.close()
    return cluster_data

"""
Upsert the clusters' confidence scores to cluster table
"""
def upsert_cluster_conf_sc(host, cluster_table_name, clusters):
    conn = get_conn(host)
    cursor = conn.cursor()
    upsert_data = []
    upsert_sql = "replace into " + cluster_table_name + "" \
                 "(cluster_id, conf_sc)" + \
                 "VALUES (?,?)"

    for cluster in clusters:
        upsert_data.append((cluster['id'], cluster['conf_sc']))

    cursor.executemany(upsert_sql, upsert_data)
    cursor.close()
    conn.close()


"""
Upsert statistics data of a project to mysql db table
"""
def upsert_statistics_to_db(project_id, host, statistics_results):
    conn = get_conn(host)
    cursor = conn.cursor()

    statistics_table_name = "T_project_analysis_result".upper()
    create_table_sql = "CREATE TABLE IF NOT EXISTS " + statistics_table_name + " (" + \
                       "project_id VARCHAR(1000) NOT NULL PRIMARY KEY ," + \
                       "cluster_size_threshold, INTEGER" + \
                       "cluster_ratio_threshold, FLOAT" + \
                       "conf_sc_threshold, FLOAT" + \
                       "spectrast_fval_threshold, FLOAT" + \
                       "prePSM_no, INTEGER" + \
                       "prePSM_not_matched_no, INTEGER" + \
                       "prePSM_high_conf_no, INTEGER, " + \
                       "prePSM_low_conf_no, INTEGER, " + \
                       "better_PSM_no, INTEGER, " + \
                       "new_PSM_no, INTEGER, " + \
                       "matched_spec_no, INTEGER, " + \
                       "matched_id_spec_no, INTEGER " + \
                       ")"
    cursor.execute(create_table_sql)

    upsert_sql = "replace into " + statistics_table_name + "(project_id, cluster_size_threshold, cluster_ratio_threshold, conf_sc_threshold, spectrast_fval_threshold, prePSM_no, prePSM_not_matched_no, prePSM_high_conf_no, prePSM_low_conf_no, better_PSM_no, new_PSM_no, matched_spec_no, matched_id_spec_no) VALUES ('%s', %d, %f, %f, %f, %d, %d, %d, %d, %d, %d, %d, %d)" % (
        statistics_results.get('project_id '),
        statistics_results.get('cluster_size_threshold'),
        statistics_results.get('cluster_ratio_threshold'),
        statistics_results.get('conf_sc_threshold'),
        # confident score threshold for accepting the recommend PSM or new identified PSM
        statistics_results.get('spectrast_fval_threshold'),  # fval threshold in spectrast matching
        statistics_results.get('prePSM_no'),
        statistics_results.get('prePSM_not_matched'),
        statistics_results.get('prePSM_high_conf_no'),
        statistics_results.get('prePSM_low_conf_no'),
        statistics_results.get('better_PSM_no'),
        statistics_results.get('new_PSM_no'),
        statistics_results.get('matched_spec_no'),
        statistics_results.get('matched_id_spec_no'),
    )
    # print(upsert_sql)

    cursor.execute(upsert_sql)

    cursor.close()
    conn.close()


"""
???Export search result of a project to mysql db table
"""
def __deprec__export_sr_to_db(project_id, search_results, cluster_data, spec_peps, host):
    pass
    # spec_peps = get_spectra_pep(project_id, host)
    # conf_sc_set = cf_calc.calculate_conf_sc(search_results, cluster_data, spec_peps, host)
    # upsert_matched_psm_table(project_id, search_results, host)
    # return conf_sc_set
    # upsert_score_psm_table(project_id, search_results, identified_spectra, conf_sc_set, cluster_data, host)

"""
Standard the json string for json load
"""
def json_stand(string):
    #transfer the string to standard json string
    if string != None:
        string = string.replace("'","\"")
        string = string.replace(": ",": \"")
        string = string.replace(",","\",")
        string = string.replace("}","\"}")
    return string


"""
Upsert search result of a project to mysql db table
"""
def upsert_matched_spec_table(project_id, matched_spec_details, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    match_table_name = "T_" + project_id.upper() + "_spec_cluster_match".upper()

    drop_table_sql = "DROP TABLE IF EXISTS " + match_table_name
    cursor.execute(drop_table_sql)

    create_table_sql = "CREATE TABLE IF NOT EXISTS " + match_table_name + " (" + \
                       "spec_title VARCHAR(1000) NOT NULL PRIMARY KEY, " + \
                       "dot FLOAT, " + \
                       "f_val FLOAT, " + \
                       "cluster_id VARCHAR(1000), " + \
                       "cluster_size INTEGER, " + \
                       "cluster_ratio FLOAT, " + \
                       "pre_seq TEXT, " + \
                       "pre_mods  TEXT, " + \
                       "recomm_seq TEXT, " + \
                       "recomm_mods  TEXT, " + \
                       "conf_sc FLOAT, " + \
                       "recomm_seq_sc FLOAT " + \
                       ")"
    cursor.execute(create_table_sql)

    upsert_data = matched_spec_details


#    upsert_sql = "replace into " + match_table_name + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    upsert_sql = "REPLACE INTO " + match_table_name.upper() + " (spec_title,dot,f_val,cluster_id,cluster_size,cluster_ratio,pre_seq,pre_mods, recomm_seq, recomm_mods, conf_sc, recomm_seq_sc)" + \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    logging.info("start to upsert_matched_spec_table, %d matched spectra is going to be imported"%(len(upsert_data)))
    try:
        cursor.executemany(upsert_sql, upsert_data)
        logging.info("Done upsert_matched_spec_table, %d matched spectra has been imported"%(len(upsert_data)))
    except Exception as e:
        logging.error("error in  upsert_matched_spec_table, failed to import the search result details(includs score and recommend sequence) in to mysql db, caused by %s"%(e))
    finally:
        cursor.close()
        conn.close()


def upsert_score_psm_table(project_id,p_score_psm_list, n_score_psm_list, new_psm_list, host):
    conn = get_conn(host)
    cursor = conn.cursor()

    p_score_psm_table_name = "T_" + project_id.upper() + "_p_score_psm".upper()

    drop_table_sql = "DROP TABLE  IF EXISTS " + p_score_psm_table_name #+ "  CASCADE "
    create_table_sql = "CREATE TABLE  " + p_score_psm_table_name + " (" + \
                       "row_id INTEGER NOT NULL PRIMARY KEY," + \
                       "conf_sc FLOAT, " + \
                       "cluster_id VARCHAR(1000), " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_ratio_str TEXT, " +\
                       "cluster_size INTEGER, " + \
                       "num_spec INTEGER, " + \
                       "spectra TEXT, " + \
                       "pre_seq TEXT, " + \
                       "pre_mods TEXT," + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)

    n_score_psm_table_name = "T_" + project_id.upper() + "_n_score_psm".upper()
    drop_table_sql = "DROP TABLE IF EXISTS " + n_score_psm_table_name #+ " CASCADE "
    create_table_sql = "CREATE TABLE " + n_score_psm_table_name + " (" + \
                       "row_id INTEGER NOT NULL PRIMARY KEY," + \
                       "conf_sc FLOAT, " + \
                       "recomm_seq_sc FLOAT, " + \
                       "cluster_id VARCHAR(1000), " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_ratio_str TEXT, " +\
                       "cluster_size INTEGER, " + \
                       "num_spec INTEGER, " + \
                       "spectra TEXT, " + \
                       "pre_seq TEXT, " + \
                       "pre_mods  TEXT, " + \
                       "recomm_seq TEXT, " + \
                       "recomm_mods  TEXT, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)

    new_psm_table_name = "T_" + project_id.upper() + "_new_psm".upper()
    drop_table_sql = "DROP TABLE IF EXISTS " + new_psm_table_name #+ " CASCADE "
    create_table_sql = "CREATE TABLE " + new_psm_table_name + " (" + \
                       "row_id INTEGER NOT NULL PRIMARY KEY," + \
                       "recomm_seq_sc FLOAT, " + \
                       "cluster_id VARCHAR(1000), " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_ratio_str TEXT, " +\
                       "cluster_size INTEGER, " + \
                       "num_spec INTEGER, " + \
                       "spectra TEXT, " + \
                       "recomm_seq TEXT, " + \
                       "recomm_mods TEXT, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)

#    upsert_p_score_psm_sql = "replace into " + p_score_psm_table_name + " values (?,?,?,?,?,?,?,?,?,?,?)"
#    upsert_n_score_psm_sql = "replace into " + n_score_psm_table_name + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
#    upsert_new_psm_sql = "replace into " + new_psm_table_name + " values (?,?,?,?,?,?,?,?,?,?,?)"

    upsert_p_score_psm_sql = "replace into " + p_score_psm_table_name.upper() + " (row_id,conf_sc,cluster_id,cluster_ratio,cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, acceptance) values(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    upsert_n_score_psm_sql = "replace into " + n_score_psm_table_name.upper() + " (row_id,conf_sc,recomm_seq_sc,cluster_id,cluster_ratio,cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, recomm_seq, recomm_mods, acceptance) values(%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
    upsert_new_psm_sql = "replace into " + new_psm_table_name.upper() + " (row_id,recomm_seq_sc,cluster_id,cluster_ratio,cluster_ratio_str, cluster_size, num_spec, spectra, recomm_seq, recomm_mods, acceptance) values(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"



    try:
        """
        for i in range(0, len(p_score_psm_list)):
            upsert_p_score_psm_sql = "replace into " + p_score_psm_table_name.upper() + " (row_id,conf_sc,cluster_id,cluster_ratio,cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, acceptance)  + \
            values(%s,%s, '%s', %s, '%s', %s, %s, '%s', '%s', '%s', %s)"%p_score_psm_list[i]
            cursor.execute(upsert_p_score_psm_sql)
        """

        cursor.executemany(upsert_p_score_psm_sql, p_score_psm_list)
        cursor.executemany(upsert_n_score_psm_sql, n_score_psm_list)
        cursor.executemany(upsert_new_psm_sql, new_psm_list)

        logging.info("%d rows has been imported in %s"%(len(p_score_psm_list), p_score_psm_table_name))
        logging.info("%d rows has been imported in %s"%(len(n_score_psm_list), n_score_psm_table_name))
        logging.info("%d rows has been imported in %s"%(len(new_psm_list), new_psm_table_name))
    except Exception as e:
        logging.error("failed to import psm tables, caused by %s"%(e))
    finally:
        cursor.close()
        conn.close()


"""
Get all matched cluster results from mysql db 
"""
def get_lib_rs_from_db(search_results, host):
    lib_result = dict()
    conn = get_conn(host)
    cursor = conn.cursor()
    clusters = dict()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        cluster_id = search_result.get('lib_spec_id')
        sql_str = "SELECT CLUSTER_RATIO, N_SPEC, N_ID_SPEC, N_UNID_SPEC, SEQUENCES_RATIOS, SEQUENCES_MODS from " + cluster_table + " WHERE CLUSTER_ID = '" + cluster_id + "'"
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
    return (clusters)




"""
Retrive identification/psms from mysql db 
"""
def retrieve_identification_from_db(project_id, host, output_file):
    conn = get_conn(host)
    cursor = conn.cursor()
    table_name = "T_" + project_id.upper() + "_psm".upper()
    sql_str = "SELECT count(*) FROM " + table_name + "";
    cursor.execute(sql_str)
    r = cursor.fetchone()
    total_n = r[0]

    psms = dict()
    offset = 0
    while (offset < total_n):
        sql_str = "SELECT SPECTRUM_TITLE, PEPTIDE_SEQUENCE, MODIFICATIONS FROM " + table_name + " LIMIT 5000 OFFSET " + str(
            offset)
        cursor.execute(sql_str)
        rs = cursor.fetchall()
        for r in rs:
            spec_title = r[0]
            id_seq = r[1]
            id_mods = r[2]
            psms[spec_title] = {'id_seq': id_seq, 'id_mods': id_mods}
        offset += 5000

    cursor.close()
    conn.close()

    if output_file != None:
        with open(output_file, "w") as o:
            o.write("spectrum_title\tsequence\n")
            for spec in psms.keys():
                o.write(spec + "\t" + str(psms.get(spec)) + "\n")
    return psms


def get_ident_no(project_id, host):
    pass
    # table_name = "T_" + project_id.upper() + "_PSM"
    # sql_str = "SELECT count(*) FROM " + table_name + "";
    # cursor.execute(sql_str)
    # r = cursor.fetchone()
    # total_n = r[0]
    # return (total_n)

"""
Create paroject analysis record table
"""
def create_project_ana_record_table(host):
    conn = get_conn(host)
    cursor = conn.cursor()

    project_ana_record_table_name = "T_project_analysis_result".upper()
    create_table_sql = "CREATE TABLE IF NOT EXISTS " + project_ana_record_table_name + " (" + \
                       "project_id VARCHAR(1000) NOT NULL PRIMARY KEY, " + \
                       "cluster_size_threshold INTEGER, " + \
                       "cluster_ratio_threshold FLOAT, " + \
                       "conf_sc_threshold FLOAT, " + \
                       "spectrast_fval_threshold FLOAT, " + \
                       "" + \
                       "prePSM_no INTEGER, " + \
                       "prePSM_not_matched_no INTEGER, " + \
                       "prePSM_high_conf_no INTEGER, " + \
                       "prePSM_low_conf_no INTEGER, " + \
                       "better_PSM_no INTEGER, " + \
                       "new_PSM_no INTEGER, " + \
                       "matched_spec_no INTEGER, " + \
                       "matched_id_spec_no INTEGER " + \
                       ")"
    cursor.execute(create_table_sql)
    cursor.close()
    conn.close()

"""
Upsert analysis status to table
"""
def upsert_analysis_status(analysis_job_accession, analysis_status, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    table_name =  "T_ANALYSIS_RECORD"
    sql_str = "replace into %s (ID, STATUS) values (%d, '%s')"%(table_name, int(analysis_job_accession[1:]), analysis_status)
    logging.info(sql_str)
    try:
        cursor.execute(sql_str)
        logging.info("Done upsert status  %s for analysis job %s"%(analysis_status, analysis_job_accession))
    except Exception as e:
        logging.error("error in upserting status  %s for analysis job %s, caused by %s"%(analysis_status, analysis_job_accession, e))
    finally:
        cursor.close()
        conn.close()

def insert_psms_to_db_from_csv(project_id, identified_spectra, psm_csv_file, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    psm_table_name = "T_%s_PSM"%(project_id.upper())
    create_table_sql = "CREATE TABLE IF NOT EXISTS " + psm_table_name + " (" + \
                       "spectrum_title VARCHAR(1000) NOT NULL PRIMARY KEY ," + \
                       "peptide_sequence TEXT," + \
                       "modifications TEXT" + \
                       ")"

    print(create_table_sql)
    cursor.execute(create_table_sql)

    query_sql = "SELECT COUNT(*) FROM %s"%(psm_table_name)
    cursor.execute(query_sql)
    n_psms_in_db = cursor.fetchone()[0]
    #todo remove this part to reduce computing time
    upsert_data = []
    for spec_title in identified_spectra.keys():
        psm = identified_spectra.get(spec_title)
        if spec_title == None or len(spec_title) < 1:
            logging.info("spec_title %s error in %s"%(spec_title,psm))
            print("spec_title %s error in %s"%(spec_title,psm))
            continue

        upsert_data.append((spec_title, psm.get('peptideSequence'), psm.get('modifications')))
    """
    upsert_sql = "replace into \"" + psm_table_name + "\"" \
                 "(spectrum_title, peptide_sequence, modifications)" + \
                 "VALUES (?,?,?)"
    """
    if n_psms_in_db >= 0.999 * len(upsert_data ):
        logging.info("the table already has all psms to upsert, quit importing from csv to mydql db !")
        return None
    logging.info("start to import identification to mysql db db, n_psms_in_db %s < len(upsert_data) %s"%(n_psms_in_db, len(upsert_data)))
    print("start to import identification to mysql db db, n_psms_in_db %s < len(upsert_data) %s"%(n_psms_in_db, len(upsert_data)))
#    cursor.executemany(upsert_sql, upsert_data)
    import_sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 ROWS;"%(psm_csv_file, psm_table_name)
    try:
        cursor.execute(import_sql)
    except Exception as e:
        logging.error("error in  insert_psms_to_db_from_csv, failed to import the psms(includs score and recommend sequence) in to mysql db, caused by %s"%(e))
    finally:
        cursor.close()
        conn.close()
#    output = os.popen("/usr/local/apache-phoenix-4.11.0-HBase-1.1-bin/bin/psql.py -t %s localhost %s"%(psm_table_name, psm_csv_file)).readlines()
#    logging.info(output)
#    print(output)

    logging.info("Done import psms to mysql db from csv, %d psm have been imported"%(len(upsert_data)))

def insert_spec_to_db_from_csv(project_id, spec_csv_file, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    # spec_table_name = "T_SPECTRUM_TEST"
    spec_table_name = "T_SPECTRUM_" + project_id.upper()

    create_table_sql = "CREATE TABLE IF NOT EXISTS " + spec_table_name + " (" + \
        "spectrum_title VARCHAR(1000) NOT NULL PRIMARY KEY ," + \
        "precursor_mz DOUBLE," + \
        "precursor_intens DOUBLE," + \
        "charge INTEGER," + \
        "peaklist_mz LONGTEXT," + \
        "peaklist_intens LONGTEXT" + \
        ")"
    cursor.execute(create_table_sql)


    query_sql = "SELECT COUNT(*) FROM %s where SPECTRUM_TITLE like '%s%%'"%(spec_table_name, project_id.upper() )
    cursor.execute(query_sql)
    n_spec_in_db = cursor.fetchone()[0]
    #todo remove this part to reduce computing time

    output = os.popen("wc -l %s"%spec_csv_file).readline().replace(spec_csv_file, "")
    n_spec_in_csv_file = int(output)

    if n_spec_in_db >= 0.999 * n_spec_in_csv_file:
        logging.info("the table already has all spec to upsert, quit importing from csv to mysql db!")
        return None
    logging.info("start to import spec to mysql db db")
    print("start to import spec to mysql db db")
#    cursor.executemany(upsert_sql, upsert_data)

#    output = os.popen("/usr/local/apache-phoenix-4.11.0-HBase-1.1-bin/bin/psql.py -t %s localhost %s"%(spec_table_name, spec_csv_file)).readlines()
#    logging.info(output)
#    print(output)
    import_sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 ROWS;"%(spec_csv_file, spec_table_name)
    try:
        cursor.execute(import_sql)
    except Exception as e:
        logging.error("error in  insert_spec_to_db_from_csv, failed to import the spectra to db from csv(includs score and recommend sequence) in to mysql db, caused by %s"%(e))
    finally:
        cursor.close()
        conn.close()

    logging.info("Done import spec to mysql db from csv, %d spec have been imported"%(n_spec_in_csv_file))

def insert_thresholds_to_record(cursor, project_id, thresholds):
    conn = get_conn('localhost')
    cursor = conn.cursor()
    project_ana_record_table_name = "T_project_analysis_result".upper()
    upsert_sql = "replace into " + project_ana_record_table_name + "(" +\
        "project_id, cluster_ratio_threshold, cluster_size_threshold, conf_sc_threshold, spectrast_fval_threshold )" +\
        "VALUES( '%s', %f, %d, %f, %f )" % (
        project_id,
        thresholds.get('cluster_ratio_threshold'),
        thresholds.get('cluster_size_threshold'),
        thresholds.get('conf_sc_threshold'),
        thresholds.get('spectrast_fval_threshold')
        )
    cursor.execute(upsert_sql)
    cursor.close()
    conn.close()

def insert_statistics_to_record(project_id, statistics_results):
    conn = get_conn('localhost')
    cursor = conn.cursor()
    project_ana_record_table_name = "T_project_analysis_result".upper()
    upsert_sql = "replace into " + project_ana_record_table_name + "(" +\
        "project_id, prePSM_no, prePSM_not_matched_no, prePSM_high_conf_no, prePSM_low_conf_no, better_PSM_no, new_PSM_no, matched_spec_no, matched_id_spec_no )" +\
        "VALUES('%s', %d, %d, %d, %d, %d, %d, %d, %d)" % (
        project_id,
        statistics_results['prePSM_no'],
        statistics_results['prePSM_not_matched_no'],
        statistics_results['prePSM_high_conf_no'],
        statistics_results['prePSM_low_conf_no'],
        statistics_results['better_PSM_no'],
        statistics_results['new_PSM_no'],
        statistics_results['matched_spec_no'],
        statistics_results['matched_id_spec_no']
        )
    cursor.execute(upsert_sql)
    logging.info("Done with sql: %s"%(upsert_sql))
    conn.commit()
    cursor.close()
    conn.close()


def test_cluster_select():
    host = "localhost"
    cluster_table = "V_CLUSTER"
    cluster_id = "14b08180-051a-4dd7-8087-6095db2704b2"
    conn = get_conn(host)
    cursor = conn.cursor()
    cluster_query_sql = "SELECT CLUSTER_RATIO, N_SPEC FROM " + cluster_table + " WHERE CLUSTER_ID = '" + cluster_id + "'"
    # print(cluster_query_sql)
    cursor.execute(cluster_query_sql)
    result = cursor.fetchone()
    # print(result[0])
    # print(result[1])


# retrieve_identification_from_db("pxd000021", "localhost", "output.txt")
#test_cluster_select()
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



#get_lib_rs_from_db(search_results, conn)
#get_spectra("PXD000021", conn)
export_sr_to_db(project_id, host, search_results)
export_ident_to_db(project_id, host, identifications)
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR(100))")
cursor.execute("replace into users VALUES (?, ?)", (1, 'admin'))
cursor.execute("replace into users VALUES (?, ?)", (2, 'user1'))
cursor.execute("replace into users VALUES (?, ?)", (3, 'user2'))
cursor.execute("SELECT * FROM users")
print(cursor.fetchall())
"""
