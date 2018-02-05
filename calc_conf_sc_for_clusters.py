#!/usr/bin/python3
import sys, os
import logging
import time
import json
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import confident_score_calc as conf_sc_calc
import phoenix_import_util as phoenix




def main():
    clusters = phoenix.get_all_clusters('localhost', 'V_CLUSTER')
    for cluster in clusters:
        scores = conf_sc_calc.calculate_conf_sc_for_a_cluster(cluster)
        cluster['conf_sc'] = json.dumps(scores)

    phoenix.upsert_cluster_conf_sc('localhost', 't_cluster_201711210', clusters)

if __name__ == "__main__":
    main()