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
import phoenix_import_util as phoenix 


def get_dict_from_string(str):
    #transfer the string to standard json string
    str = str.replace("'","\"")
    # str = str.replace(": ",": \"")
    # str = str.replace(",","\",")
    # str = str.replace("}","\"}")
    r_dict = json.loads(str)
    return r_dict


"""
Calculate the confident scores for Original Pep-Spec-Match
Based on our scoring model
"""
def calculate_conf_sc(search_results, spectra_peps, host):
    clusters = phoenix.get_lib_rs_from_phoenix(search_results, host)
    conf_scs = {}
    print("Calculating confident scores")
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

    print(conf_scs)
    return conf_scs

#calculate a pep_seq's confident score, if recommend a new pep seq, return it's score too
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
    confident_score = normalized_n_spec * (this_seq_ratio - sqrt_of_others)
#        print("conf_sc %f for pep seq %s in cluster %s " % (confident_score, pep_seq, lib_spec_id))
#        print("normalized_n_spec %f * (this_seq_ratio %f - sqrt_of_others %f)" % (normalized_n_spec , this_seq_ratio , sqrt_of_others))
    if this_seq_ratio ==  0.5 and confident_score == 0:  #
        confident_score = - 0.1  #penalizing score -0.1 for (0.5 0.5)
    better_confident_score = None
    if no_pre_identification:
        pep_seq = "R_NEW_" + pep_seq
    else:
        if confident_score < 0 and max_seq_ratio > 0.5:
            ##recommend a new seq, and calculate it's conf score
            other_ratios_2.pop(max_seq)
            pep_seq = "R_Better_" + max_seq
            sum_sqr_of_others = 0.0
            for other_ratio in other_ratios_2.values():
                sum_sqr_of_others += pow(other_ratio,2)
            sqrt_of_others = math.sqrt(sum_sqr_of_others)
            better_confident_score = normalized_n_spec * (max_seq_ratio - sqrt_of_others)
        else:
            pep_seq = "PRE_" + pep_seq
    return (confident_score, pep_seq, better_confident_score)
