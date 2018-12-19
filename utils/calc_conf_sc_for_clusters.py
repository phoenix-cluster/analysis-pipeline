#!/usr/bin/python3
import sys, os
import logging
import time
import json
file_dir = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.abspath(os.path.join(file_dir, os.pardir))
sys.path.append(file_dir)
sys.path.append(parent_path)
import confident_score_calc as conf_sc_calc
import phoenix_storage_access as phoenix
import mysql_storage_access as mysql_acc




def main():
    calculate_to_mysql()
    pass

def calculate_to_phoenix():
    clusters = phoenix.get_all_clusters('localhost', 'V_CLUSTER')
    for cluster in clusters:
        scores = conf_sc_calc.calculate_conf_sc_for_a_cluster(cluster)
        cluster['conf_sc'] = json.dumps(scores)

    phoenix.upsert_cluster_conf_sc('localhost', 't_cluster_201711210', clusters)

def calculate_to_mysql():
    clusters = mysql_acc.get_all_clusters('T_CLUSTER_TEST')
    for cluster in clusters:
        scores = conf_sc_calc.calculate_conf_sc_for_a_cluster(cluster)
        cluster['conf_sc'] = json.dumps(scores)

    mysql_acc.upsert_cluster_conf_sc('T_CLUSTER_TEST', clusters)


if __name__ == "__main__":
    main()
