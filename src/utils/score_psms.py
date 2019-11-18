
import sys
#sys.path.insert(0, "./py-venv/lib/python3.6/site-packages")
import logging
import json, csv


#read cluster_taxid map from csv file
def read_cluster_taxid_map(cluster_taxid_csv):
    cluster_taxid_map = dict()
    with open(cluster_taxid_csv, mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            cluster_taxid_map[row[0]] = ", ".join(row[1:])

    return cluster_taxid_map



"""
Build 3 score psm list from cluster_data and matched_spec_details_dict. 
"""
def build_score_psm_list(cluster_data, thresholds, matched_spec_details_dict, cluster_taxid_csv_path):
    #remove the psms violents the conf_sc/recomm_seq_sc threshods
    #group the matched spec by cluster
    spectra_matched_to_cluster = dict()
    unid_spec_matched_to_cluster = dict()  # cluster_id as the key

    cluster_taxid_map = read_cluster_taxid_map(cluster_taxid_csv_path)

    removed_spec_no = 0
    retail_spec_no = 0
    logging.info("Start to  process %d matched spectra in build_score_psm_list function."%(len(matched_spec_details_dict)))
    for spec_title in matched_spec_details_dict.keys():
        spec_match = matched_spec_details_dict.get(spec_title)
        f_val = spec_match.get('f_val', 0)
        cluster_ratio = float(spec_match.get('cluster_ratio',0.0))
        cluster_size = int(spec_match.get('cluster_size',0))
        pre_seq = spec_match.get('pre_seq', '')
        conf_score = float(spec_match.get('conf_sc'))
        recomm_seq_sc = float(spec_match.get('recomm_seq_sc'))
        cluster_id = spec_match.get('cluster_id')

        if f_val < thresholds.get('spectrast_fval_threshold') or \
           cluster_ratio < thresholds.get('cluster_ratio_threshold') or \
           cluster_size < thresholds.get('cluster_size_threshold') :
            removed_spec_no += 1
            continue
        if conf_score and conf_score > 0 \
            and conf_score < thresholds.get('conf_sc_threshold'):  # For pre PSMs with postive confidence score, ignore the PSMs below threshold
            removed_spec_no += 1
            continue
        if conf_score and conf_score < 0 and recomm_seq_sc \
            and recomm_seq_sc < thresholds.get('conf_sc_threshold'):  # For pre PSMs with negtive confidence score or unidentified (-1 for conf_sc), ignore the PSMs whose recommend seq's score is below threshold
            removed_spec_no += 1
            continue
        if pre_seq == None or pre_seq == '':
            if recomm_seq_sc < thresholds.get('conf_sc_threshold'): # For the unidientfied spec, ignore the recommend New PSMs whose recommend seq's score is below threshold
                removed_spec_no += 1
                continue
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        matched_spectra.append(spec_title)
        spectra_matched_to_cluster[cluster_id] = matched_spectra
        retail_spec_no += 1
    logging.info("%d spectra have be filtered by thresholds" %(removed_spec_no))
    logging.info("%d spectra have be retailed" %(retail_spec_no))

    p_score_psm_list = list()
    n_score_psm_list = list()
    new_psm_list = list()


    p_score_seq_taxid_dict = dict()
    n_score_seq_taxid_dict = dict()
    new_seq_taxid_dict = dict()

    for cluster_id in spectra_matched_to_cluster.keys():
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        # logging.info("cluster %s matched to %d spec"%(cluster_id, len(matched_spectra)))
        cluster_ratio_str = cluster_data.get(cluster_id).get('seqs_ratios')
        matched_peptides = dict()
        #group the scored (previously identified) PSMs by peptide sequence(and modifications)
        #or group the scored (previously unidentified) PSMs by peptide sequence(and modifications)
        # if(cluster_id.startswith("50668639")):
        #     logging.info(matched_spectra)
        n_id = 0
        n_unid = 0

        #get the taxid info for each cluster
        seq_taxid = cluster_taxid_map.get(cluster_id).replace("'", "\"")
        seq_taxid_map = json.loads(seq_taxid)
        logging.debug("get taxids for %d clusters"%(len(seq_taxid_map)))

        #pick the identified/unidentified spectra for one cluster
        for matched_spec in matched_spectra:
            spec_match = matched_spec_details_dict.get(matched_spec, None)
            pre_seq = spec_match.get('pre_seq')
            pre_mods = spec_match.get('pre_mods', None)
            if pre_seq:
                n_id += 1
                pep_seq_mods_str = pre_seq
                if pre_mods != None:
                    pep_seq_mods_str += "||" + pre_mods
                pep_spectra = matched_peptides.get(pep_seq_mods_str, [])
                pep_spectra.append(matched_spec)
                matched_peptides[pep_seq_mods_str] = pep_spectra
            else:
                n_unid += 1
                matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id, [])
                matched_unid_spec.append(matched_spec)
                unid_spec_matched_to_cluster[cluster_id] = matched_unid_spec
        # logging.info("matched %d identified spectra, %d unidentified spectra"%(n_id, n_unid))
        n_id = 0
        n_id_in_p_score_list = 0

        #deal the identified spectra in the order of peptides
        for pep_seq_mods_str in matched_peptides.keys():
            pep_spectra = matched_peptides.get(pep_seq_mods_str, [])
            n_id += len(pep_spectra)
            spec1 = pep_spectra[0]  # get the first spec in list
            conf_score = float(matched_spec_details_dict.get(spec1).get('conf_sc'))
            recomm_seq_sc = 0.0
            if matched_spec_details_dict.get(spec1).get('recomm_seq_sc'):
                recomm_seq_sc = float(matched_spec_details_dict.get(spec1).get('recomm_seq_sc'))
            num_spec = len(pep_spectra)
            spectra = "||".join(pep_spectra)
            spec_match = matched_spec_details_dict.get(spec1)
            pre_seq = spec_match.get('pre_seq')
            pre_mods = spec_match.get('pre_mods', None)
            cluster_ratio = float(spec_match.get('cluster_ratio'))
            cluster_size = int(spec_match.get('cluster_size'))
            if conf_score > 0:
                pre_seq_taxid = str(seq_taxid_map.get(pre_seq)).replace("'","")
                logging.debug("get preseq_taxid %s for pre_seq %s"%(pre_seq_taxid, pre_seq))
                p_score_psm_list.append((len(p_score_psm_list)+1, conf_score, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, pre_seq_taxid, 0))
                # logging.info("add num_spec: %d" % num_spec)
                n_id_in_p_score_list += num_spec

                ###deal taxids ###
                if pre_seq_taxid.lower() != 'none':
                    seq_taxids = pre_seq_taxid[1:-1].replace(' ', '')  #remove the "[]" and the space
                    seq_taxid_list = seq_taxids.split(",")
                    logging.debug("p sc seq %s"%seq_taxid_list)
                    for taxid in seq_taxid_list:
                        if taxid == '' or taxid.lower() == 'none' or taxid.lower() == 'unknown':
                            continue
                        psm_no = p_score_seq_taxid_dict.get(taxid, 0)
                        psm_no = psm_no + 1
                        p_score_seq_taxid_dict[taxid] = psm_no
                ###deal taxids ###

            if conf_score < 0:
                recomm_seq = spec_match.get('recomm_seq')
                recomm_seq = recomm_seq.replace("R_Better_", "")
                recomm_mods = spec_match.get('recomm_mods')
                recomm_seq_taxid = str(seq_taxid_map.get(recomm_seq)).replace("'","")
                logging.debug("get recommseq_taxid %s for recomm_seq %s"%(recomm_seq_taxid, recomm_seq))
                n_score_psm_list.append((len(n_score_psm_list)+1, conf_score, recomm_seq_sc, cluster_id, cluster_ratio,
                                         cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods,
                                         recomm_seq, recomm_mods, recomm_seq_taxid, 0))

                ###deal taxids ###
                if recomm_seq_taxid.lower() != 'none':
                    seq_taxids = recomm_seq_taxid[1:-1].replace(' ', '')  #remove the "[]" and the space
                    seq_taxid_list = seq_taxids.split(",")
                    logging.debug("n sc seq %s"%seq_taxid_list)
                    for taxid in seq_taxid_list:
                        if taxid == '' or taxid.lower() == 'none' or taxid.lower() == 'unknown':
                            continue
                        logging.debug("n_score_seq_taxid_dict: %s"%n_score_seq_taxid_dict)
                        psm_no = n_score_seq_taxid_dict.get(taxid, 0)
                        psm_no = psm_no + 1
                        n_score_seq_taxid_dict[taxid] = psm_no
                ###deal taxids ###
        # logging.info("cluster %s matched to %d id spec in matched_peptides"%(cluster_id, n_id))
        # logging.info("%d spec in p_score_list"%(n_id_in_p_score_list))

        #group the recommend new psms for one cluster
        matched_unid_spec = unid_spec_matched_to_cluster.get(cluster_id)
        if matched_unid_spec != None and len(matched_unid_spec) > 0:
            num_spec = len(matched_unid_spec)
            spec1 = matched_unid_spec[0]  # get the first spec in list
            spec_match = matched_spec_details_dict.get(spec1)
            recomm_seq_sc = float(spec_match.get('recomm_seq_sc'))
            cluster_ratio = float(spec_match.get('cluster_ratio'))
            cluster_size = int(spec_match.get('cluster_size'))
            recomm_seq = spec_match.get('recomm_seq')
            recomm_seq = recomm_seq.replace("R_NEW_", "")
            recomm_mods = spec_match.get('recomm_mods')
            spectra = "||".join(matched_unid_spec)

            recomm_seq_taxid = str(seq_taxid_map.get(recomm_seq)).replace("'","")
            ###deal taxids ###
            if recomm_seq_taxid.lower() != 'none':
                seq_taxids = recomm_seq_taxid[1:-1].replace(' ', '')  #remove the "[]" and the space
                seq_taxid_list = seq_taxids.split(",")
                logging.debug("new seq %s"%seq_taxid_list)
                for taxid in seq_taxid_list:
                     if taxid == '' or taxid.lower() == 'none' or taxid.lower() == 'unknown':
                            continue
                     psm_no = new_seq_taxid_dict.get(taxid, 0)
                     psm_no = psm_no + 1
                     new_seq_taxid_dict[taxid] = psm_no
            ###deal taxids ###

            logging.debug("get recommseq_taxid %s for recomm_seq %s"%(recomm_seq_taxid, recomm_seq))
            new_psm_list.append((len(new_psm_list)+1, recomm_seq_sc, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, recomm_seq, recomm_mods, recomm_seq_taxid, 0))

    taxid_statistics_dict = {
        "negscore": n_score_seq_taxid_dict,
        "posscore": p_score_seq_taxid_dict,
        "newid": new_seq_taxid_dict,
    }

    logging.debug(taxid_statistics_dict)

    return (p_score_psm_list, n_score_psm_list, new_psm_list, taxid_statistics_dict)

