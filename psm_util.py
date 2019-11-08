import sys, os
import csv,json
import logging

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
    #if none identification data found
    if not identified_spectra or len(identified_spectra) < 1:
        return None

    matched_spec = list()
    # psm_dict = dict()
    logging.info("start to build matched spec details")
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        dot = float(search_result.get('dot'))
        f_val = float(search_result.get('fval'))

        cluster_id = search_result.get('lib_spec_id')
        cluster = cluster_data.get(cluster_id)
        if cluster == None:
            logging.debug("Null cluster for: %s, which may small than 5"%(cluster_id))
            continue
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

            # il_seq = pre_seq.replace('I', 'L')

            seq_ratio = float(seqs_ratios_dict.get(pre_seq, -1))

            if abs(seq_ratio - cluster_ratio) <= 0.001:  # this seq matches to the highest score seq
                recomm_seq = "PRE_"
                recomm_mods = ""
                conf_sc = float(conf_sc_dict.get(pre_seq, -1))
                if abs(conf_sc + 1) <=0.001:  #pre_seq has matched seq_ratio but not in conf_sc_dict, error
                    logging.error("Found no conf_sc for pre_seq %s" %(pre_seq))
                    logging.error("From conf_sc_dict %s" %(conf_sc_dict))
                recomm_seq_sc = conf_sc
            elif pre_seq in conf_sc_dict.keys():  # this seq matches to the lower score seq
                recomm_seq = "R_Better_" + max_sc_seq
                if mods_dict:
                    recomm_mods = mods_dict.get(max_sc_seq)
                else:
                    recomm_mods = ""
                conf_sc = float(conf_sc_dict.get(pre_seq))
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
    if matched_spec_dict is None:
        return None
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
        if matched_spec is None or len(matched_spec) < 1:
            return
        for row in matched_spec:
            w.writerow(row)
    logging.info("Done write_matched_spec_to_csv, %d matched details have been written"%len(matched_spec))


def read_matched_spec_from_csv(csv_file):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
        return None
    with open(csv_file, 'r') as f:
        new_dict = {}
        reader = csv.reader(f, delimiter=',',skipinitialspace=True)
        fieldnames = next(reader)
        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',', skipinitialspace=True)
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

def read_identification_from_csv(csv_files):
    new_dict = {}
    for csv_file in csv_files:
        if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
            print("no csv file found: %s"%(csv_file))
            logging.info("no csv file found: %s"%(csv_file))
            return None
        print("start to read identification from csv")
        logging.info("start to read identification from csv")
        with open(csv_file, 'r') as f:
            fieldnames = ['spectrumTitle', 'peptideSequence', 'modifications']

            reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',', skipinitialspace=True)
            for row in reader:
                spec_title = row.pop('spectrumTitle')
                new_dict[spec_title] = row
        logging.info("%d identifed peptide has been read"%len(new_dict))
    return new_dict
