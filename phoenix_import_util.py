import phoenixdb
import time
import math
import os, sys, re, json

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
    with conn.cursor() as cursor:
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
        spectra_peps[spectra_title] = {"seq": pep_seq, "mods": mod_seq}
    print("got %d identified spectra" % (len(spectra_peps)))
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
Read cluster data from phoenix tables
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


def upsert_cluster_conf_sc(host, cluster_table_name, clusters):
    conn = get_conn(host)
    cursor = conn.cursor()
    upsert_data = []
    upsert_sql = "UPSERT INTO \"" + cluster_table_name + "\"" \
                 "(cluster_id, conf_sc)" + \
                 "VALUES (?,?)"

    for cluster in clusters:
        upsert_data.append((cluster['id'], cluster['conf_sc']))

    cursor.executemany(upsert_sql, upsert_data)

    cursor.close()
    conn.close()


"""
Upsert statistics data of a project to phoenix/hbase table
"""


def upsert_statistics_to_phoenix(project_id, host, statistics_results):
    conn = get_conn(host)
    cursor = conn.cursor()

    statistics_table_name = "T_statistics_"
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + statistics_table_name.upper() + "\" (" + \
                       "project_id VARCHAR NOT NULL PRIMARY KEY ," + \
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

    upsert_sql = "UPSERT INTO " + statistics_table_name + " VALUES ('%s', %d, %f, %f, %f, %d, %d, %d, %d, %d, %d, %d, %d)" % (
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
Export search result of a project to phoenix/hbase table
"""


def export_sr_to_phoenix(project_id, search_results, cluster_data, spec_peps, host):
    # spec_peps = get_spectra_pep(project_id, host)
    conf_sc_set = cf_calc.calculate_conf_sc(search_results, cluster_data, spec_peps, host)
    # upsert_matched_psm_table(project_id, search_results, host)
    return conf_sc_set
    # upsert_score_psm_table(project_id, search_results, identified_spectra, conf_sc_set, cluster_data, host)


def json_stand(string):
    #transfer the string to standard json string
    if string != None:
        string = string.replace("'","\"")
        string = string.replace(": ",": \"")
        string = string.replace(",","\",")
        string = string.replace("}","\"}")
    return string

def upsert_matched_psm_table_new(project_id, matched_spec_details, host, date):
    conn = get_conn(host)
    cursor = conn.cursor()

    match_table_name = "T_" + project_id + "_spec_cluster_match_"
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + match_table_name.upper() + "\" (" + \
                       "spec_title VARCHAR NOT NULL PRIMARY KEY, " + \
                       "dot FLOAT, " + \
                       "f_val FLOAT, " + \
                       "cluster_id VARCHAR, " + \
                       "cluster_size INTEGER, " + \
                       "cluster_ratio FLOAT, " + \
                       "pre_seq VARCHAR, " + \
                       "pre_mods  VARCHAR, " + \
                       "recomm_seq VARCHAR, " + \
                       "recomm_mods  VARCHAR, " + \
                       "conf_sc FLOAT, " + \
                       "recomm_seq_sc FLOAT " + \
                       ")"
    cursor.execute(create_table_sql)

    upsert_sql = "UPSERT INTO " + match_table_name.upper() + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    upsert_data = matched_spec_details
    try:
        cursor.executemany(upsert_sql, upsert_data)
    finally:
        cursor.close()
        conn.close()




# def upsert_matched_psm_table(project_id, search_results, conf_sc_set, cluster_data, host):
def upsert_matched_psm_table(project_id, search_results, identified_specta, cluster_data, host, date):
    conn = get_conn(host)
    cursor = conn.cursor()

    match_table_name = "T_" + project_id + "_spec_cluster_match_" + date
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + match_table_name.upper() + "\" (" + \
                       "spec_title VARCHAR NOT NULL PRIMARY KEY, " + \
                       "dot FLOAT, " + \
                       "f_val FLOAT, " + \
                       "cluster_id VARCHAR, " + \
                       "cluster_size INTEGER, " + \
                       "cluster_ratio FLOAT, " + \
                       "pre_seq VARCHAR, " + \
                       "pre_mods  VARCHAR, " + \
                       "recomm_seq VARCHAR, " + \
                       "recomm_mods  VARCHAR, " + \
                       "conf_sc FLOAT, " + \
                       "recomm_seq_sc FLOAT " + \
                       ")"
    cursor.execute(create_table_sql)

    upsert_sql = "UPSERT INTO " + match_table_name.upper() + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    upsert_data = list()
    # psm_dict = dict()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        dot = float(search_result.get('dot'))
        f_val = float(search_result.get('fval'))

        cluster_id = search_result.get('lib_spec_id')
        cluster = cluster_data.get(cluster_id)
        cluster_ratio = float(cluster.get('ratio'))
        cluster_size = cluster.get('size')
        cluster_conf_sc_str = json_stand(cluster.get('conf_sc'))
        seqs_ratios_str = json_stand(cluster.get('seqs_ratios'))
        seqs_mods_str = json_stand(cluster.get('seqs_mods'))

        conf_sc_dict = None
        seqs_ratios_dict = None
        mods_dict = None
        if cluster_conf_sc_str :
            conf_sc_dict = json.loads(cluster_conf_sc_str)
        if seqs_ratios_str:
            seqs_ratios_dict = json.loads(seqs_ratios_str)
        if seqs_mods_str:
            mods_dict = json.loads(seqs_mods_str)

        max_sc = 0.0
        max_sc_seq = ''
        if conf_sc_dict == None:
            print("cluster %s do not has confidence score str"%(cluster_id))
            continue
        for each_seq in conf_sc_dict.keys():
            if float(conf_sc_dict.get(each_seq)) > max_sc:
                max_sc = float(conf_sc_dict.get(each_seq))
                max_sc_seq = each_seq

        identification = identified_specta.get(spec_title)
        recomm_seq = ""
        recomm_mods = ""
        conf_sc = 0.0
        recomm_seq_sc = 0.0
        if identification:
            pre_seq = identification.get('id_seq')
            pre_mods = identification.get('id_mods')
            il_seq = pre_seq.replace('I', 'L')

            seq_ratio = seqs_ratios_dict.get(il_seq, -1)

            if seq_ratio == cluster_ratio:      #this seq matches to the highest score seq
                recomm_seq = "PRE_"
                recomm_mods = ""
                conf_sc = float(conf_sc_dict.get(il_seq))
                recomm_seq_sc = conf_sc
            elif il_seq in conf_sc_dict.keys():  #this seq matches to the lower score seq
                recomm_seq = "R_Better_" + max_sc_seq
                if mods_dict:
                    recomm_mods = mods_dict.get(max_sc_seq)
                else:
                    recomm_mods = ""
                conf_sc = float(conf_sc_dict.get(il_seq))
                recomm_seq_sc = max_sc
        else:                              #this seq matches non seq in the cluster
            pre_seq = ''
            pre_mods = ''
            recomm_seq = "R_NEW_" + max_sc_seq
            if mods_dict:
                recomm_mods = mods_dict.get(max_sc_seq)
            else:
                recomm_mods = ""
            conf_sc = 0
            recomm_seq_sc = max_sc
        upsert_data.append((spec_title, dot, f_val, cluster_id, cluster_size, cluster_ratio, pre_seq, pre_mods,
                            recomm_seq, recomm_mods, conf_sc, recomm_seq_sc))

        # psm = dict()
        # psm['pre_seq'] = pre_seq
        # psm['pre_mods'] = pre_mods
        # psm['f_val'] = f_val
        # psm['cluster_id'] = cluster_id
        # psm['cluster_size'] = cluster_size
        # psm['cluster_ratio'] = cluster_ratio
        # psm['conf_sc'] = conf_sc
        # psm['recomm_seq_sc'] = recomm_seq_sc
        # psm_dict[spec_title] = psm

    cursor.executemany(upsert_sql, upsert_data)
    cursor.close()
    conn.close()


def build_score_psm_table(project_id, search_results, identified_spectra, conf_sc_set, thresholds, cluster_data, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    spectra_matched_to_cluster = dict()
    unid_spec_matched_to_cluster = dict()  # cluster_id as the key
    score_psm_table_name = "T_" + project_id + "_score_psm_" + time.strftime("%Y%m%d")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + score_psm_table_name.upper() + "\" (" + \
                       "id INTEGER NOT NULL PRIMARY KEY," + \
                       "cluster_id VARCHAR, " + \
                       "pep_seq VARCHAR, " + \
                       "pep_mods VARCHAR, " + \
                       "conf_sc FLOAT, " + \
                       "recomm_seq_sc FLOAT, " + \
                       "f_val FLOAT, " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_size INTEGER, " + \
                       "recommend_pep VARCHAR, " + \
                       "recommend_mods VARCHAR, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(create_table_sql)

    new_psm_table_name = "T_" + project_id + "_new_psm_" + time.strftime("%Y%m%d")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + new_psm_table_name.upper() + "\" (" + \
                       "id INTEGER NOT NULL PRIMARY KEY," + \
                       "cluster_id VARCHAR, " + \
                       "recomm_seq_sc FLOAT, " + \
                       "f_val FLOAT, " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_size INTEGER, " + \
                       "recommend_pep VARCHAR, " + \
                       "recommend_mods VARCHAR, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(create_table_sql)

    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)

        f_val = float(search_results.get(spec_title).get('fval'))
        if f_val < thresholds.get('spectrast_fval_threshold'):
            continue

        cluster_id = search_result.get('lib_spec_id')
        cluster = cluster_data.get(cluster_id)
        cluster_ratio = cluster.get('ratio')
        cluster_size = cluster.get('size')
        if cluster_ratio < thresholds.get('cluster_ratio_threshold') or cluster_size < thresholds.get(
                'cluster_size_threshold'):
            continue

        conf_score = conf_sc_set.get(spec_title).get('conf_score')
        recomm_seq_sc = conf_sc_set.get(spec_title).get('recomm_seq_score')
        if conf_score and conf_score > 0 and conf_score < thresholds.get(
                'conf_sc_threshold'):  # For pre PSMs with postive confidence score, ignore the PSMs below threshold
            continue
        if conf_score and conf_score < 0 and recomm_seq_sc and recomm_seq_sc < thresholds.get(
                'conf_sc_threshold'):  # For pre PSMs with negtive confidence score or unidentified (-1 for conf_sc), ignore the PSMs whose recommend seq's score is below threshold
            continue
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        matched_spectra.append(spec_title)
        spectra_matched_to_cluster[cluster_id] = matched_spectra
    id_row_num = 1
    unid_row_num = 1
    for cluster_id in spectra_matched_to_cluster.keys():
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        matched_peptides = dict()
        for matched_spec in matched_spectra:
            pep_seq_data = identified_spectra.get(matched_spec, None)
            if pep_seq_data:
                pep_seq_data_str = pep_seq_data.get('id_seq')
                if pep_seq_data.get('id_mods') != None:
                    pep_seq_data_str += "||" + pep_seq_data.get('id_mods')
                pep_spectra = matched_peptides.get(pep_seq_data_str, [])
                pep_spectra.append(matched_spec)
                matched_peptides[pep_seq_data_str] = pep_spectra
            else:
                matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id, [])
                matched_unid_spec.append(matched_spec)
                unid_spec_matched_to_cluster[cluster_id] = matched_unid_spec
        # print("got %d pep_seq for cluster %s"%(len(matched_peptides), cluster_id))
        # print(conf_sc_set)
        for pep_seq_data_str in matched_peptides.keys():
            pep_spectra = matched_peptides.get(pep_seq_data_str, [])
            spec1 = pep_spectra[0]  # get the first spec in list
            conf_score = conf_sc_set.get(spec1).get('conf_score')
            recomm_seq_sc = -1.0
            if conf_sc_set.get(spec1).get('recomm_seq_score'):
                recomm_seq_sc = float(conf_sc_set.get(spec1).get('recomm_seq_score'))
            f_val = float(search_results.get(spec1).get('fval'))
            num_spec = len(pep_spectra)
            spectra = "||".join(pep_spectra)
            recommend_pep_seq = conf_sc_set.get(spec1).get('recommend_pep_seq')
            recommend_mods = conf_sc_set.get(spec1).get('recommend_mods')
            cluster = cluster_data.get(cluster_id)
            cluster_ratio = cluster.get('ratio')
            cluster_size = cluster.get('size')

            pep_seq_data = pep_seq_data_str.split("||")
            pep_seq = pep_seq_data[0]
            pep_mods = ""
            if len(pep_seq_data) > 1:
                pep_mods = pep_seq_data[1]
            upsert_sql = "UPSERT INTO " + score_psm_table_name.upper() + " VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(upsert_sql, ( \
                id_row_num, cluster_id, pep_seq, pep_mods, conf_score, recomm_seq_sc, f_val, \
                cluster_ratio, cluster_size, recommend_pep_seq, recommend_mods, num_spec, spectra, 0 \
                ))
            # print(upsert_sql)
            # print("upsert pep seq %s in cluster %s, with spectra:%s"%(pep_seq, cluster_id, spectra))
            id_row_num += 1

        matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id)
        if matched_unid_spec != None and len(matched_unid_spec) > 0:
            spec1 = matched_unid_spec[0]  # get the first spec in list
            conf_score = conf_sc_set.get(spec1).get('conf_score')
            f_val = float(search_results.get(spec1).get('fval'))
            num_spec = len(matched_unid_spec)
            spectra = "||".join(matched_unid_spec)
            recommend_pep_seq = conf_sc_set.get(spec1).get('recommend_pep_seq')
            recommend_mods = conf_sc_set.get(spec1).get('recommend_mods')
            cluster = cluster_data.get(cluster_id)
            cluster_ratio = cluster.get('ratio')
            cluster_size = cluster.get('size')
            upsert_sql = "UPSERT INTO " + new_psm_table_name.upper() + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(upsert_sql, ( \
                unid_row_num, cluster_id, conf_score, f_val, \
                cluster_ratio, cluster_size, recommend_pep_seq, recommend_mods, num_spec, spectra, 0 \
                ))
            unid_row_num += 1
    cursor.close()
    conn.close()

def build_score_psm_table_new(project_id, cluster_data, thresholds, psm_dict, host, date):
    conn = get_conn(host)
    cursor = conn.cursor()
    if date == None:
        date = time.strftime("%Y%m%d")

    p_score_psm_table_name = "T_" + project_id + "_p_score_psm_" + date

    drop_table_sql = "DROP TABLE  IF EXISTS \"" + p_score_psm_table_name.upper() + "\"  CASCADE "
    create_table_sql = "CREATE TABLE  \"" + p_score_psm_table_name.upper() + "\" (" + \
                       "row_id INTEGER NOT NULL PRIMARY KEY," + \
                       "conf_sc FLOAT, " + \
                       "cluster_id VARCHAR, " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_ratio_str VARCHAR, " +\
                       "cluster_size INTEGER, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR, " + \
                       "pre_seq VARCHAR, " + \
                       "pre_mods  VARCHAR, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)

    n_score_psm_table_name = "T_" + project_id + "_n_score_psm_" + date
    drop_table_sql = "DROP TABLE IF EXISTS \"" + n_score_psm_table_name.upper() + "\" CASCADE "
    create_table_sql = "CREATE TABLE \"" + n_score_psm_table_name.upper() + "\" (" + \
                       "row_id INTEGER NOT NULL PRIMARY KEY," + \
                       "conf_sc FLOAT, " + \
                       "recomm_seq_sc FLOAT, " + \
                       "cluster_id VARCHAR, " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_ratio_str VARCHAR, " +\
                       "cluster_size INTEGER, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR, " + \
                       "pre_seq VARCHAR, " + \
                       "pre_mods  VARCHAR, " + \
                       "recomm_seq VARCHAR, " + \
                       "recomm_mods  VARCHAR, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)

    new_psm_table_name = "T_" + project_id + "_new_psm_" + date
    drop_table_sql = "DROP TABLE IF EXISTS \"" + new_psm_table_name.upper() + "\" CASCADE "
    create_table_sql = "CREATE TABLE \"" + new_psm_table_name.upper() + "\" (" + \
                       "row_id INTEGER NOT NULL PRIMARY KEY," + \
                       "recomm_seq_sc FLOAT, " + \
                       "cluster_id VARCHAR, " + \
                       "cluster_ratio FLOAT, " + \
                       "cluster_ratio_str VARCHAR, " +\
                       "cluster_size INTEGER, " + \
                       "num_spec INTEGER, " + \
                       "spectra VARCHAR, " + \
                       "recomm_seq VARCHAR, " + \
                       "recomm_mods  VARCHAR, " + \
                       "acceptance INTEGER" + \
                       ")"
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)

    if psm_dict == None:
        match_table_name = "T_" + project_id + "_spec_cluster_match_" + date
        select_sql = "select spec_title, pre_seq, pre_mods,recomm_seq, recomm_mods, f_val, cluster_id, " \
                     "cluster_size, cluster_ratio, conf_sc, recomm_seq_sc from %s" \
                     " where f_val >= %f and cluster_ratio >= %f and cluster_size >= %d" % (
                        match_table_name.upper(), thresholds.get('spectrast_fval_threshold'),
                        thresholds.get('cluster_ratio_threshold'), thresholds.get('cluster_size_threshold')
                    )
        cursor.execute(select_sql)
        results = cursor.fetchall()

        for rs in results:
            psm = dict()
            spec_title = rs[0]
            psm['pre_seq'] = rs[1]
            psm['pre_mods'] = rs[2]
            psm['recomm_seq'] = rs[3]
            psm['recomm_mods'] = rs[4]
            psm['f_val'] = rs[5]
            psm['cluster_id'] = rs[6]
            psm['cluster_size'] = rs[7]
            psm['cluster_ratio'] = rs[8]
            psm['conf_sc'] = rs[9]
            psm['recomm_seq_sc'] = rs[10]
            psm_dict[spec_title] = psm

    #remove the psms violents the conf_sc/recomm_seq_sc threshods
    #group the matched spec by cluster
    spectra_matched_to_cluster = dict()
    unid_spec_matched_to_cluster = dict()  # cluster_id as the key
    for spec_title in psm_dict.keys():
        psm = psm_dict.get(spec_title)
        pre_seq = psm.get('pre_seq', None)
        conf_score = float(psm.get('conf_sc'))
        recomm_seq_sc = float(psm.get('recomm_seq_sc'))
        cluster_id = psm.get('cluster_id')
        if conf_score and conf_score > 0 \
            and conf_score < thresholds.get('conf_sc_threshold'):  # For pre PSMs with postive confidence score, ignore the PSMs below threshold
            continue
        if conf_score and conf_score < 0 and recomm_seq_sc \
            and recomm_seq_sc < thresholds.get('conf_sc_threshold'):  # For pre PSMs with negtive confidence score or unidentified (-1 for conf_sc), ignore the PSMs whose recommend seq's score is below threshold
            continue
        if pre_seq == None and recomm_seq_sc < thresholds.get('conf_sc_threshold'): # For the unidientfied spec, ignore the recommend New PSMs whose recommend seq's score is below threshold
            continue
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        matched_spectra.append(spec_title)
        spectra_matched_to_cluster[cluster_id] = matched_spectra


    p_score_psm_list = list()
    n_score_psm_list = list()
    new_psm_list = list()
    for cluster_id in spectra_matched_to_cluster.keys():
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        cluster_ratio_str = cluster_data.get(cluster_id).get('seqs_ratios')
        matched_peptides = dict()
        #group the scored (previously identified) PSMs by peptide sequence(and modifications)
        #or group the scored (previously unidentified) PSMs by peptide sequence(and modifications)
        for matched_spec in matched_spectra:
            psm = psm_dict.get(matched_spec, None)
            pre_seq = psm.get('pre_seq')
            pre_mods = psm.get('pre_mods', None)
            if pre_seq:
                pep_seq_mods_str = pre_seq
                if pre_mods != None:
                    pep_seq_mods_str += "||" + pre_mods
                pep_spectra = matched_peptides.get(pep_seq_mods_str, [])
                pep_spectra.append(matched_spec)
                matched_peptides[pep_seq_mods_str] = pep_spectra
            else:
                matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id, [])
                matched_unid_spec.append(matched_spec)
                unid_spec_matched_to_cluster[cluster_id] = matched_unid_spec

        for pep_seq_mods_str in matched_peptides.keys():
            pep_spectra = matched_peptides.get(pep_seq_mods_str, [])
            spec1 = pep_spectra[0]  # get the first spec in list
            conf_score = float(psm_dict.get(spec1).get('conf_sc'))
            recomm_seq_sc = 0.0
            if psm_dict.get(spec1).get('recomm_seq_sc'):
                recomm_seq_sc = float(psm_dict.get(spec1).get('recomm_seq_sc'))
            num_spec = len(pep_spectra)
            spectra = "||".join(pep_spectra)
            psm = psm_dict.get(spec1)
            pre_seq = psm.get('pre_seq')
            pre_mods = psm.get('pre_mods', None)
            cluster_ratio = float(psm.get('cluster_ratio'))
            cluster_size = int(psm.get('cluster_size'))
            if conf_score > 0:
                p_score_psm_list.append((len(p_score_psm_list)+1, conf_score, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, 0))
            if conf_score < 0:
                recomm_seq = psm.get('recomm_seq')
                recomm_mods = psm.get('recomm_mods')
                n_score_psm_list.append((len(n_score_psm_list)+1, conf_score, recomm_seq_sc, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, recomm_seq, recomm_mods, 0))

        #group the recommed new psms for one cluster
        matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id)
        if matched_unid_spec != None and len(matched_unid_spec) > 0:
            num_spec = len(matched_unid_spec)
            spec1 = matched_unid_spec[0]  # get the first spec in list
            psm = psm_dict.get(spec1)
            recomm_seq_sc = float(psm.get('recomm_seq_sc'))
            cluster_ratio = float(psm.get('cluster_ratio'))
            cluster_size = int(psm.get('cluster_size'))
            recomm_seq = psm.get('recomm_seq')
            recomm_mods = psm.get('recomm_mods')
            spectra = "||".join(matched_unid_spec)

            new_psm_list.append((len(new_psm_list)+1, recomm_seq_sc, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, recomm_seq, recomm_mods, 0))

    upsert_p_score_psm_sql = "upsert into " + p_score_psm_table_name.upper() + " values (?,?,?,?,?,?,?,?,?,?,?)"
    upsert_n_score_psm_sql = "upsert into " + n_score_psm_table_name.upper() + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    upsert_new_psm_sql = "upsert into " + new_psm_table_name.upper() + " values (?,?,?,?,?,?,?,?,?,?,?)"

    try:
        cursor.executemany(upsert_p_score_psm_sql, p_score_psm_list)
        cursor.executemany(upsert_n_score_psm_sql, n_score_psm_list)
        cursor.executemany(upsert_new_psm_sql, new_psm_list)
    finally:
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
        sql_str = "SELECT CLUSTER_RATIO, N_SPEC, N_ID_SPEC, N_UNID_SPEC, SEQUENCES_RATIOS, SEQUENCES_MODS from \"" + cluster_table + "\" WHERE CLUSTER_ID = '" + cluster_id + "'"
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
Export identification result of a project to phoenix/hbase table from mysql db
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
                       "pep_seq VARCHAR " + \
                       ")"
    conn = get_conn(host)
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    for spec_title in identifications.keys():
        upsert_sql = "UPSERT INTO " + table_name + " VALUES (?, ?)"
        cursor.execute(upsert_sql, (spec_title, identifications[spec_title]))

    cursor.execute("SELECT count(*) FROM " + table_name)
    print(cursor.fetchone())

    cursor.close()
    conn.close()

    end = time.time()
    print("end phoenix inserting at " + str(end))
    print("totally time " + str(end - start))


"""
Export cluster data  to phoenix/hbase table from mysql db
"""


def import_clusters_to_phoenix(connection):
    old_cluster_table = "201504_3"
    #    ratio_threshold = "0.618"
    size_threshold = 2
    clusters = dict()

    select_str = "SELECT cluster_id,cluster_ratio, n_spec FROM %s WHERE n_spec>=%d" % \
                 (old_cluster_table, size_threshold)
    with connection.cursor() as cursor:
        cursor.execute(select_str)
    results = cursor.fetchall()

    start = time.time()
    print("start phoenix inserting at " + str(start))
    table_name = "identifications_" + project_id + "_" + time.strftime("%d%m%Y")
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + table_name.upper() + "\" (" + \
                       "spec_title VARCHAR NOT NULL PRIMARY KEY , " + \
                       "pep_seq VARCHAR " + \
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
    conn = get_conn(host)
    cursor = conn.cursor()
    table_name = "T_" + project_id.upper() + "_PSM"
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
    table_name = "T_" + project_id.upper() + "_PSM"
    sql_str = "SELECT count(*) FROM " + table_name + "";
    cursor.execute(sql_str)
    r = cursor.fetchone()
    total_n = r[0]
    return (total_n)


def create_project_ana_record_table(host):
    conn = get_conn(host)
    cursor = conn.cursor()

    project_ana_record_table_name = "t_project_analysis_record"
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + project_ana_record_table_name.upper() + "\" (" + \
                       "project_id VARCHAR NOT NULL PRIMARY KEY, " + \
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


def test_cluster_select():
    host = "localhost"
    cluster_table = "V_CLUSTER"
    cluster_id = "14b08180-051a-4dd7-8087-6095db2704b2"
    conn = get_conn(host)
    cursor = conn.cursor()
    cluster_query_sql = "SELECT CLUSTER_RATIO, N_SPEC FROM \"" + cluster_table + "\" WHERE CLUSTER_ID = '" + cluster_id + "'"
    # print(cluster_query_sql)
    cursor.execute(cluster_query_sql)
    result = cursor.fetchone()
    print(result[0])
    print(result[1])


# retrieve_identification_from_phoenix("pxd000021", "localhost", "output.txt")
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
