#!/usr/bin/python3
import sys, os
import logging
import time
import json
import csv
file_dir = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.abspath(os.path.join(file_dir, os.pardir))
sys.path.append(file_dir)
sys.path.append(parent_path)
import confident_score_calc as conf_sc_calc
import phoenix_storage_access as phoenix
import mysql_storage_access as mysql_acc




def main():
    # calculate_to_mysql()
    calculate_to_csv()
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


def calculate_to_csv():
    origin_cluster_csv_file = "clusters_min5_old.csv"
    new_cluster_csv_file = "clusters_min5.csv"
    with open(origin_cluster_csv_file, 'r') as f:
        reader = csv.reader(f, delimiter=',',skipinitialspace=True)
        fieldnames = next(reader)
        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',', skipinitialspace=True)
        clusters = list()
        for row in reader:
            cluster = dict(row)
            scores = conf_sc_calc.calculate_conf_sc_for_a_cluster(cluster)
            cluster['conf_sc'] = scores
            clusters.append(cluster)

        with open(new_cluster_csv_file, 'w') as f:
            w = csv.DictWriter(f, fieldnames)
            w.writeheader()
            w.writerows(clusters)


if __name__ == "__main__":
    main()
