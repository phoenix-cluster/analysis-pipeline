
import logging

"""
Build 3 score psm list from cluster_data and matched_spec_details_dict. 
"""
def build_score_psm_list(cluster_data, thresholds, matched_spec_details_dict):
    #remove the psms violents the conf_sc/recomm_seq_sc threshods
    #group the matched spec by cluster
    spectra_matched_to_cluster = dict()
    unid_spec_matched_to_cluster = dict()  # cluster_id as the key
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
    for cluster_id in spectra_matched_to_cluster.keys():
        matched_spectra = spectra_matched_to_cluster.get(cluster_id, [])
        # logging.info("cluster %s matched to %d spec"%(cluster_id, len(matched_spectra)))
        cluster_ratio_str = cluster_data.get(cluster_id).get('seqs_ratios')
        matched_peptides = dict()
        #group the scored (previously identified) PSMs by peptide sequence(and modifications)
        #or group the scored (previously unidentified) PSMs by peptide sequence(and modifications)
        if(cluster_id.startswith("50668639")):
            logging.info(matched_spectra)
        n_id = 0
        n_unid = 0
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
        # logging.info("%d id, %d unid"%(n_id, n_unid))
        n_id = 0
        n_id_in_p_score_list = 0

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
                p_score_psm_list.append((len(p_score_psm_list)+1, conf_score, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, 0))
                # logging.info("add num_spec: %d" % num_spec)
                n_id_in_p_score_list += num_spec
            if conf_score < 0:
                recomm_seq = spec_match.get('recomm_seq')
                recomm_mods = spec_match.get('recomm_mods')
                n_score_psm_list.append((len(n_score_psm_list)+1, conf_score, recomm_seq_sc, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, pre_seq, pre_mods, recomm_seq, recomm_mods, 0))

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
            recomm_mods = spec_match.get('recomm_mods')
            spectra = "||".join(matched_unid_spec)

            new_psm_list.append((len(new_psm_list)+1, recomm_seq_sc, cluster_id, cluster_ratio, cluster_ratio_str, cluster_size, num_spec, spectra, recomm_seq, recomm_mods, 0))


    return (p_score_psm_list, n_score_psm_list, new_psm_list)
