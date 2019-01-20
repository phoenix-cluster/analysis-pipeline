#!/usr/bin/python3
import sys, os
import logging
import time
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import confident_score_calc as conf_sc_calc




def main():
    cluster_id = '000001f5-cb21-4db9-9ece-6ea8ab3a7ded'
    cluster_ratio = 0.71428573
    cluster_ratio = 0.5
    n_spec = 21
    seq_ratios_str = "{'TLAMASTDGVKR': 0.2857142857142857, 'TLAMASTDGVQR': 0.7142857142857143}"
    seq_ratios_str =  "{'WLGMNTSNYQRSCEDAPSMAFLQRR': 0.5, 'DAEEALSQTLDTLVDMLK': 0.5}"
    conf_scs = conf_sc_calc.calculate_conf_sc_for_a_cluster(n_spec,seq_ratios_str,cluster_ratio,cluster_id)
    print(str(conf_scs))
    print(seq_ratios_str)

if __name__ == "__main__":
    main()
