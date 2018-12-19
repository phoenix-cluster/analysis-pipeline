# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 09:59:27 2017

@author: baimi
"""

import math
import os,sys,re,json
import traceback

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)


def get_dict_from_string(str):
    #transfer the string to standard json string
    str = str.replace("'","\"")
    # str = str.replace(": ",": \"")
    # str = str.replace(",","\",")
    # str = str.replace("}","\"}")
    r_dict = json.loads(str)
    return r_dict


"""
Calculate the confidence scores for Original Pep-Spec-Match
Based on our scoring model
"""
def calculate_conf_sc(search_results, clusters, spectra_peps, host):
    # clusters = phoenix.get_lib_rs_from_phoenix(search_results, host)
    conf_scs = {}
    print("Calculating confidence scores")
    for spec_title in search_results.keys():
        search_result = search_results.get(spec_title)
        lib_spec_id = search_result.get('lib_spec_id')
        dot = search_result.get('dot')
        fval = search_result.get('fval')

        cluster = clusters.get(lib_spec_id)
        #todo comment it. this is only for debug
        if cluster == None:
            print("Warnning! Got null matched cluster for " + lib_spec_id)
            continue
        ratio = cluster.get('ratio')
        n_spec = cluster.get('n_spec') 
        seqs_ratios_str = cluster.get('seqs_ratios')

        if n_spec > 1000:    #we assume n>=1000. the contribution of n_spec is al the same
            cutted_n_spec = 1000
        else :
            cutted_n_spec = n_spec

        spec_data = spectra_peps.get(spec_title)
        pep_seqs = list()
        mod_seqs = list()
        if spec_data == None or spec_data  == "" or spec_data.get("seq") == None or spec_data.get("seq") == "":
            pep_seqs.append("RECOMMEND")
#            raise  Exception("Got None peptide sequence for %s" % spec_title)
        else:
            spec_pep_str = spec_data.get("seq")
            spec_mod_str = spec_data.get("mods")
            spec_pep_str = spec_pep_str.replace("I","L") #replace acid I to L
            spec_pep_str = spec_pep_str.replace("||", ",")
            spec_pep_str = spec_pep_str.replace("|", ",")
            spec_pep_str = spec_pep_str.rstrip()
            pep_seqs = spec_pep_str.split(",")
            if spec_mod_str != None:
                mod_seqs = spec_mod_str.split(";")
                if len(pep_seqs) != len(mod_seqs):
                    print("Error! length of pep_seqs != length of mod_seqs")
                    os.exit(1)
        # seq_scores = list()
        max_score = -1000000.0
        max_score_seq = ""
        for pep_seq in pep_seqs:
            (seq_score,returned_pep_seq, recomm_seq_score) = calculate_conf_sc_for_a_seq(pep_seq, cutted_n_spec, seqs_ratios_str, ratio, lib_spec_id)#the returned pep_seq could be recommend sequence for the non identified spectrum
            if seq_score > max_score:
                max_score = seq_score
                accpeted_recomm_seq_score = recomm_seq_score
                max_score_seq = returned_pep_seq
            # seq_scores.append(seq_score)
        # if accpeted_recomm_seq_score:
        #     print("accepted_recomm_seq_score of seq : " + max_score_seq + " is " + str(accpeted_recomm_seq_score))
        if len(pep_seqs)>1:
            print("This spectrum has multiple PSMs, we chose the max score %f for %s"%(max_score, max_score_seq))
        try:
            mod_seq = ""
            if max_score_seq.startswith("R_NEW_") or max_score_seq.startswith("R_Better"):
                clean_seq = max_score_seq.replace("R_NEW_","").replace("R_Better_","")
                seqs_mods_str = cluster.get('seqs_mods')
                seq_mods_map = get_dict_from_string(seqs_mods_str)
                seq_mods_map2 = dict()
                for seq_key in seq_mods_map.keys():
                    new_key = seq_key.replace("I","L")
                    seq_mods_map2[new_key] = seq_mods_map.get(seq_key)
                mod_seq = seq_mods_map2.get(clean_seq)
            if max_score_seq.startswith("PRE_"):    
                clean_seq = max_score_seq.replace("PRE_","")
                seq_index = pep_seqs.index(clean_seq)
                if len(mod_seqs) > seq_index:
                    mod_seq = mod_seqs[seq_index]
            conf_scs[spec_title] = {"conf_score":max_score, "recommend_pep_seq":max_score_seq, "recommend_mods":mod_seq,
                                    "recomm_seq_score": accpeted_recomm_seq_score}
        except Exception as ex:
            traceback.print_exc()
            print(ex)
            print(str(pep_seqs))
            print(str(spec_data))
            exit(1)

    # print(conf_scs)
    return conf_scs

#calculate a pep_seq's confidence score, if recommend a new pep seq, return it's score too
def calculate_conf_sc_for_a_seq(pep_seq, n_spec, seqs_ratios_str, ratio, lib_spec_id):
    # print("gonna to calculate conf_sc for %s, %d, %s, %f, %s"%(pep_seq, n_spec, seqs_ratios_str, ratio, lib_spec_id))
    normalized_n_spec = math.log(n_spec)/math.log(1000)
    no_pre_identification = False
    # allUpper = re.compile(r'^[A-Z]')
    # if allUpper.match(spectrum_pep):
    allUpper = re.compile(r'[^A-Z]')
    if allUpper.match(pep_seq):
        print(allUpper.match(pep_seq))
        raise Exception("Peptide sequence is not all upper case letter: " + pep_seq)\

    #transfer the string to standard json string
    seqs_ratios_str = seqs_ratios_str.replace("'","\"")
    seqs_ratios_str = seqs_ratios_str.replace(": ",": \"")
    seqs_ratios_str = seqs_ratios_str.replace(",","\",")
    seqs_ratios_str = seqs_ratios_str.replace("}","\"}")
    seqs_ratios = json.loads(seqs_ratios_str)


    this_seq_ratio = 0.0
    max_seq_ratio = 0.0
    max_seq = ""
    other_ratios = dict()
    for seq in seqs_ratios.keys():
        other_ratios[seq] = float(seqs_ratios.get(seq))
        if other_ratios[seq] > max_seq_ratio:
            max_seq_ratio = float(other_ratios[seq])
            max_seq = seq

    other_ratios_2 = other_ratios
    if max_seq_ratio > ratio + 0.01 or max_seq_ratio < ratio - 0.01:
        raise Exception("The max-seq_ratio is not equal to the ratio in database with cluster %s : %s, %f"%(lib_spec_id, seqs_ratios_str, ratio))

    try:
        if pep_seq == "RECOMMEND":
            pep_seq = max_seq
            no_pre_identification = True
        this_seq_ratio = seqs_ratios.get(pep_seq)
        other_ratios.pop(pep_seq)
    except KeyError as ex:
#            print("No such key: '%s'" % ex)
        #assign a new ratio for this seq, and adjust the others
        if this_seq_ratio ==None or this_seq_ratio == "":
            this_seq_ratio = 1.0/(n_spec + 1)
            adjust_factor = n_spec/(n_spec + 1.0)
            for akey in other_ratios.keys():
                other_ratios[akey] *= adjust_factor
        #raise Exception("Got None ratio for this peptide sequence %s from seqs_ratios %s in cluster %s" % (pep_seq, seqs_ratios_str, lib_spec_id))

    this_seq_ratio = float(this_seq_ratio)

    #some time, multiple sequences could be assigned to one spectrum, cause a sum of ratios more than 1.
    #for this situation, we pick the other ratios from small to big, to make a set of ratios to "1".
    #here we modified the other_ratios list
    sum_ratio = 0.0
    for temp_value in seqs_ratios.values():
        sum_ratio += float(temp_value)
    if sum_ratio > 1.001:
        new_other_ratios = dict()
        max_sum_others = 1 - this_seq_ratio
        i = 0
        # print("sum of all ratios: " + str(sum_ratio))
        #print(other_ratios)
        if max_sum_others > 0:
            for temp_ratio in sorted(other_ratios.values()):
                if sum(new_other_ratios.values()) < max_sum_others:
                    new_other_ratios[str(i)] = temp_ratio
                    # print("add a new ratio " + str(temp_ratio))
                    i += 1
            offset = sum(new_other_ratios.values()) - max_sum_others
            # print("offset is" + str(offset))
            new_other_ratios[str(len(new_other_ratios)-1)] -= offset
            other_ratios = new_other_ratios

    sum_sqr_of_others = 0.0
    for other_ratio in other_ratios.values():
        sum_sqr_of_others += pow(other_ratio,2)
    sqrt_of_others = math.sqrt(sum_sqr_of_others)
    confidence_score = normalized_n_spec * (this_seq_ratio - sqrt_of_others)
#        print("conf_sc %f for pep seq %s in cluster %s " % (confidence_score, pep_seq, lib_spec_id))
#        print("normalized_n_spec %f * (this_seq_ratio %f - sqrt_of_others %f)" % (normalized_n_spec , this_seq_ratio , sqrt_of_others))
    if this_seq_ratio ==  0.5 and confidence_score == 0:  #
        confidence_score = - 0.1  #penalizing score -0.1 for (0.5 0.5)
    better_confidence_score = None
    if no_pre_identification:
        pep_seq = "R_NEW_" + pep_seq
        better_confidence_score = confidence_score
    else:
        if confidence_score < 0 and max_seq_ratio > 0.5:
            ##recommend a new seq, and calculate it's conf score
            try:
                other_ratios_2.pop(max_seq)
            except KeyError as ex:
                print("max_seq: %s"%(max_seq))
                print("other_ratios_2: %s"%(str(other_ratios_2)))
            pep_seq = "R_Better_" + max_seq
            sum_sqr_of_others = 0.0
            for other_ratio in other_ratios_2.values():
                sum_sqr_of_others += pow(other_ratio,2)
            sqrt_of_others = math.sqrt(sum_sqr_of_others)
            better_confidence_score = normalized_n_spec * (max_seq_ratio - sqrt_of_others)
        else:
            pep_seq = "PRE_" + pep_seq
    return (confidence_score, pep_seq, better_confidence_score)

#calculate all pep_seq's confidence scores in a cluster, and generate a conf_score for non-exist pep in cluster, if need to recommend a new pep seq, return it's max score pep
def calculate_conf_sc_for_a_cluster(cluster):
    # print("gonna to calculate conf_sc for %s, %d, %s, %f, %s"%(pep_seq, n_spec, seqs_ratios_str, ratio, lib_spec_id))
    cluster_id = cluster['id']
    ratio = cluster['ratio']
    n_spec = cluster['size']
    seqs_ratios_str = cluster['seqs_ratios']

    if n_spec > 1000:
        n_spec = 1000
    normalized_n_spec = math.log(n_spec)/math.log(1000)

    #transfer the string to standard json string
    seqs_ratios_str = seqs_ratios_str.replace("'","\"")
    seqs_ratios_str = seqs_ratios_str.replace(": ",": \"")
    seqs_ratios_str = seqs_ratios_str.replace(",","\",")
    seqs_ratios_str = seqs_ratios_str.replace("}","\"}")
    seqs_ratios = json.loads(seqs_ratios_str)


    max_seq_ratio = 0.0
    max_seq = ""
    ratios = dict()
    sum_ratio = 0.0
    allUpper = re.compile(r'[^A-Z]')
    il_ratios = dict()
    for seq in seqs_ratios.keys():
        if allUpper.match(seq):
            print(allUpper.match(seq))
            raise Exception("Peptide sequence is not all upper case letter: " + seq)

        ratios[seq] = float(seqs_ratios.get(seq))
        sum_ratio += ratios[seq]
        # if ratios[seq] > max_seq_ratio:
        #     max_seq_ratio = float(ratios[seq])
        #     max_seq = seq

        #I/L peptides in one cluster, should prepare the il_ratios for i/l peptide
        il_seq = seq.replace("I", "L")
        il_ratio_score = il_ratios.get(il_seq, 0)
        il_ratios[il_seq] = il_ratio_score + float(seqs_ratios.get(seq))

    # if max_seq_ratio > ratio + 0.01 or max_seq_ratio < ratio - 0.01:
    #     print("The max-seq_ratio is not equal to the ratio in database with cluster %s : %s, %f"%(cluster_id, seqs_ratios_str, ratio))

    #some time, multiple sequences could be assigned to one spectrum, cause a sum of ratios more than 1.
    #for this situation, we set the ratios by dividing make the all ratios to be 1
    if sum_ratio > 1.001:
        # print("old ratios: " + str(ratios) + " for cluster " + cluster_id)
        for il_seq in il_ratios:
            il_ratios[il_seq] = il_ratios[il_seq]/sum_ratio
        # print("new ratios divied by a factor: " + str(ratios) + " for cluster " + cluster_id)

    #calculating the confidence scores for the seq in cluster
    confidence_scores = dict()
    for seq in ratios.keys():
        il_seq = seq.replace("I", "L")
        other_ratios = il_ratios.copy()
        this_ratio = other_ratios.pop(il_seq)
        sum_sqr_of_others = 0.0
        for other_ratio in other_ratios.values():
            sum_sqr_of_others += pow(other_ratio,2)
        sqrt_of_others = math.sqrt(sum_sqr_of_others)
        confidence_score = normalized_n_spec * (this_ratio - sqrt_of_others)
        if this_ratio ==  0.5 and confidence_score == 0:  #
            confidence_score = - 0.1  #penalizing score -0.1 for (0.5 0.5)
        confidence_scores[seq] = confidence_score

    #calculating the confidence scores for the non-exist seq
    this_ratio = 1.0/(n_spec + 1)
    adjust_factor = n_spec/(n_spec + 1.0)
    for akey in il_ratios.keys():
        il_ratios[akey] *= adjust_factor
    sum_sqr_of_others = 0.0
    for other_ratio in il_ratios.values():
        sum_sqr_of_others += pow(other_ratio,2)
    sqrt_of_others = math.sqrt(sum_sqr_of_others)
    confidence_score = normalized_n_spec * (this_ratio - sqrt_of_others)
    confidence_scores['_NonSEQ'] = confidence_score

    return (confidence_scores)



