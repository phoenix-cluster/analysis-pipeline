import sys, os
import csv,json
import logging

import phoenixdb

"""
Get connection 
"""
def get_conn(host):
    database_url = 'http://' + host + ':8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)
    return conn

def json_stand(string):
    #transfer the string to standard json string
    if string != None:
        string = string.replace("'","\"")
        string = string.replace(": ",": \"")
        string = string.replace(",","\",")
        string = string.replace("}","\"}")
    return string

"""
build matched spec details data from search_results, identified spectra(to add seq info) and cluster data(to add cluster info).
"""
def build_matched_spec(search_results, identified_spectra, cluster_data):
    matched_spec = list()
    # psm_dict = dict()
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        dot = float(search_result.get('dot'))
        f_val = float(search_result.get('fval'))

        cluster_id = search_result.get('lib_spec_id')
        cluster = cluster_data.get(cluster_id)
        cluster_ratio = float(cluster.get('ratio'))
        cluster_size = int(cluster.get('size'))
        cluster_conf_sc_str = json_stand(cluster.get('conf_sc'))
        seqs_ratios_str = json_stand(cluster.get('seqs_ratios'))
        seqs_mods_str = cluster.get('seqs_mods')

        conf_sc_dict = None
        seqs_ratios_dict = None
        mods_dict = None
        if cluster_conf_sc_str:
            conf_sc_dict = json.loads(cluster_conf_sc_str)
        if seqs_ratios_str:
            seqs_ratios_dict = json.loads(seqs_ratios_str)
        if seqs_mods_str:
            mods_dict = json.loads(seqs_mods_str)

        max_sc = 0.0
        max_sc_seq = ''
        if conf_sc_dict == None:
            logging.info("cluster %s do not has confidence score str" % (cluster_id))
            continue
        for each_seq in conf_sc_dict.keys():
            if float(conf_sc_dict.get(each_seq)) > max_sc:
                max_sc = float(conf_sc_dict.get(each_seq))
                max_sc_seq = each_seq

        identification = identified_spectra.get(spec_title)
        recomm_seq = ""
        recomm_mods = ""
        conf_sc = 0.0
        recomm_seq_sc = 0.0
        if identification:
            # pre_seq = identification.get('id_seq')
            # pre_mods = identification.get('id_mods')
            pre_seq = identification.get('peptideSequence')
            if pre_seq == None:
                pre_seq = identification.get('id_seq')

            pre_mods = identification.get('modifications')
            if pre_mods == None:
                pre_mods = identification.get('id_mods')

            il_seq = pre_seq.replace('I', 'L')

            seq_ratio = seqs_ratios_dict.get(il_seq, -1)

            if seq_ratio == cluster_ratio:  # this seq matches to the highest score seq
                recomm_seq = "PRE_"
                recomm_mods = ""
                conf_sc = float(conf_sc_dict.get(il_seq))
                recomm_seq_sc = conf_sc
            elif il_seq in conf_sc_dict.keys():  # this seq matches to the lower score seq
                recomm_seq = "R_Better_" + max_sc_seq
                if mods_dict:
                    recomm_mods = mods_dict.get(max_sc_seq)
                else:
                    recomm_mods = ""
                conf_sc = float(conf_sc_dict.get(il_seq))
                recomm_seq_sc = max_sc
        else:  # this seq matches non seq in the cluster
            pre_seq = ''
            pre_mods = ''
            recomm_seq = "R_NEW_" + max_sc_seq
            if mods_dict:
                recomm_mods = mods_dict.get(max_sc_seq)
            else:
                recomm_mods = ""
            conf_sc = 0
            recomm_seq_sc = max_sc
        matched_spec.append((spec_title, dot, f_val, cluster_id, cluster_size, cluster_ratio, pre_seq, pre_mods,
                            recomm_seq, recomm_mods, conf_sc, recomm_seq_sc))
    logging.info("Done build_matched_spec, build %d matched spec from %d search results and %d identified spectra."%(len(matched_spec), len(search_results), len(identified_spectra)))
    return matched_spec

"""
translate matched details from dict to list
"""
def trans_matched_spec_to_list(matched_spec_dict):
    matched_spec = list()
    for spec_title in matched_spec_dict.keys():
        matching = matched_spec_dict.get(spec_title)
        matched_spec.append((spec_title,
                             matching.get('dot'),
                             matching.get('f_val'),
                             matching.get('cluster_id'),
                             matching.get('cluster_size'),
                             matching.get('cluster_ratio'),
                             matching.get('pre_seq'),
                             matching.get('pre_mods'),
                             matching.get('recomm_seq'),
                             matching.get('recomm_mods'),
                             matching.get('conf_sc'),
                             matching.get('recomm_seq_sc')
                             ))
    logging.info("Done translate matched_spec from dict to list")
    return matched_spec

def write_matched_spec_to_csv(matched_spec, output_file):
    field_names = [
        "spec_title",
        "dot",
        "f_val",
        "cluster_id",
        "cluster_size",
        "cluster_ratio",
        "pre_seq",
        "pre_mods",
        "recomm_seq",
        "recomm_mods",
        "conf_sc",
        "recomm_seq_sc",
    ]

    with open(output_file, 'w', newline="") as f:
        w = csv.writer(f)
        w.writerow(field_names)
        for row in matched_spec:
            w.writerow(row)
    logging.info("Done write_matched_spec_to_csv, %d matched details have been written"%len(matched_spec))

def insert_psms_to_phoenix_from_csv(project_id, identified_spectra, psm_csv_file, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    psm_table_name = "T_%s_PSM"%(project_id)
    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + psm_table_name.upper() + "\" (" + \
                       "spectrum_title VARCHAR NOT NULL PRIMARY KEY ," + \
                       "peptide_sequence VARCHAR," + \
                       "modifications VARCHAR" + \
                       ")"
    cursor.execute(create_table_sql)

    query_sql = "SELECT COUNT(*) FROM %s"%(psm_table_name.upper())
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
    upsert_sql = "UPSERT INTO \"" + psm_table_name.upper() + "\"" \
                 "(spectrum_title, peptide_sequence, modifications)" + \
                 "VALUES (?,?,?)"

    if n_psms_in_db >= len(upsert_data):
        logging.info("the table already has all psms to upsert, quit importing from csv to phoenix!")
        return None
    logging.info("start to import identification to phoenix db, n_psms_in_db %s < len(upsert_data) %s"%(n_psms_in_db, len(upsert_data)))
    print("start to import identification to phoenix db, n_psms_in_db %s < len(upsert_data) %s"%(n_psms_in_db, len(upsert_data)))
#    cursor.executemany(upsert_sql, upsert_data)
    output = os.popen("/usr/local/apache-phoenix-4.11.0-HBase-1.1-bin/bin/psql.py -t %s localhost %s"%(psm_table_name, psm_csv_file)).readlines()
    logging.info(output)
    print(output)

    cursor.close()
    conn.close()

    logging.info("Done import psms to phoenix from csv, %d psm have been imported"%(len(upsert_data)))

def insert_spec_to_phoenix_from_csv(project_id, spec_csv_file, host):
    conn = get_conn(host)
    cursor = conn.cursor()
    spec_table_name = "T_SPECTRUM_TEST"

    create_table_sql = "CREATE TABLE IF NOT EXISTS \"" + spec_table_name.upper() + "\" (" + \
        "spectrum_title VARCHAR NOT NULL PRIMARY KEY ," + \
        "precursor_mz FLOAT," + \
        "precursor_intens FLOAT," + \
        "charge INTEGER," + \
        "peaklist_mz VARCHAR," + \
        "peaklist_intens VARCHAR" + \
        ")"
    cursor.execute(create_table_sql)


    query_sql = "SELECT COUNT(*) FROM %s where SPECTRUM_TITLE like '%s%%'"%(spec_table_name.upper(), project_id.upper() )
    cursor.execute(query_sql)
    n_spec_in_db = cursor.fetchone()[0]
    #todo remove this part to reduce computing time

    output = os.popen("wc -l %s"%spec_csv_file).readline().replace(spec_csv_file, "")
    n_spec_in_csv_file = int(output)

    if n_spec_in_db == n_spec_in_csv_file:
        logging.info("the table already has all spec to upsert, quit importing from csv to phoenix!")
        return None
    logging.info("start to import spec to phoenix db")
    print("start to import spec to phoenix db")
#    cursor.executemany(upsert_sql, upsert_data)

    output = os.popen("/usr/local/apache-phoenix-4.11.0-HBase-1.1-bin/bin/psql.py -t %s localhost %s"%(spec_table_name, spec_csv_file)).readlines()
    logging.info(output)
    print(output)

    cursor.close()
    conn.close()

    logging.info("Done import spec to phoenix from csv, %d spec have been imported"%(n_spec_in_csv_file))



def read_matched_spec_from_csv(csv_file):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
        return None
    with open(csv_file, 'r') as f:
        new_dict = {}
        reader = csv.reader(f, delimiter=',')
        fieldnames = next(reader)
        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')
        for row in reader:
            spec_title = row.pop('spec_title')
            row['dot'] = float(row.get("dot"))
            row['f_val'] = float(row.get("f_val"))
            row['cluster_id'] = row.get("cluster_id")
            row['cluster_size'] = int(row.get("cluster_size"))
            row['cluster_ratio'] = float(row.get("cluster_ratio"))
            row['pre_seq'] = row.get("pre_seq")
            row['pre_mods'] = row.get("pre_mods")
            row['recomm_seq'] = row.get("recomm_seq")
            row['recomm_mods'] = row.get("recomm_mods")
            row['conf_sc'] = float(row.get("conf_sc"))
            row['recomm_seq_sc'] = float(row.get("recomm_seq_sc"))

            new_dict[spec_title] = row
    logging.info("Done read_matched_spec_from_csv, %s matched spectra have been readed from csv file"%len(new_dict))
    return new_dict

def read_identification_from_csv(csv_file):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
        print("no csv found: %s"%(csv_file))
        logging.info("no csv found: %s"%(csv_file))
        return None
    print("start to read identification from csv")
    logging.info("start to read identification from csv")
    with open(csv_file, 'r') as f:
        new_dict = {}
        reader = csv.reader(f, delimiter=',')
        fieldnames = ['spectrumTitle', 'peptideSequence', 'modifications']

        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')
        for row in reader:
            spec_title = row.pop('spectrumTitle')

            new_dict[spec_title] = row
    logging.info("%d identifed peptide has been read"%len(new_dict))
    return new_dict
