import sys, os
import csv,json

def json_stand(string):
    #transfer the string to standard json string
    if string != None:
        string = string.replace("'","\"")
        string = string.replace(": ",": \"")
        string = string.replace(",","\",")
        string = string.replace("}","\"}")
    return string


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
            print("cluster %s do not has confidence score str" % (cluster_id))
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
            pre_mods = identification.get('modifications')

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
            # row['cluster_id'] = row.get["cluster_id"]
            row['cluster_size'] = int(row.get("cluster_size"))
            row['cluster_ratio'] = float(row.get("cluster_ratio"))
            # row['pre_seq'] = row.get("pre_seq")
            # row['pre_mods'] = row.get("pre_mods")
            # row['recomm_seq'] = row.get("recomm_seq")
            # row['recomm_mods'] = row.get("recomm_mods")
            row['conf_sc'] = float(row.get("conf_sc"))
            row['recomm_seq_sc'] = float(row.get("recomm_seq_sc"))

            new_dict[spec_title] = row
    return new_dict
