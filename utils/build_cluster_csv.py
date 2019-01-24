import sys, os
import csv

"""
This program export cluster info from phoenix db to csv file.
"""
file_dir = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.abspath(os.path.join(file_dir, os.pardir))
sys.path.append(file_dir)
sys.path.append(parent_path)
# import phoenix_storage_access as phoenix
import mysql_storage_access as mysql_acc


def write_to_csv(cluster_data, output_file, fieldnames):
    with open(output_file, 'w', newline="") as f:
        w = csv.writer(f)
        fieldnames = list(cluster_data[0].keys())
        fieldnames.remove('id')
        fieldnames.insert(0, 'id')
        w.writerow(fieldnames)
        for cluster in cluster_data:
            row = list()
            for key in fieldnames:
                value = cluster.get(key)
                if isinstance(value, str):
                    value = value.replace("\"", "\'")
                row.append(value)
            w.writerow(row)


"""
#read the cluster library from csv file
"""


def read_csv(csv_file):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
        return None
    with open(csv_file, 'r') as f:
        new_dict = dict()
        reader = csv.reader(f, delimiter=',')
        fieldnames = next(reader)
        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')
        for row in reader:
            cluster_id = row.pop('id')
            ratio = row['ratio']
            size = row['size']
            seqs_ratios = row['seqs_ratios']
            conf_sc = row['conf_sc']
            seqs_mods = row['seqs_mods'].replace("\'", "\"")

            cluster = dict()
            cluster['ratio'] = ratio
            cluster['size'] = size
            cluster['seqs_ratios'] = seqs_ratios
            cluster['conf_sc'] = conf_sc
            cluster['seqs_mods'] = seqs_mods
            new_dict[cluster_id] = cluster
    return new_dict





def main():
    cluster_table = "T_CLUSTER_TEST"
    csv_file = 'clusters_min5.csv'
    fieldnames = [
        'id',
        'ratio',
        'size',
        'seqs_ratios',
        'conf_sc',
        'seqs_mods',
    ]
    # cluster_data = phoenix.get_all_clusters(host, cluster_table, 5)
    cluster_data = mysql_acc.get_all_clusters(cluster_table, 5)
    write_to_csv(cluster_data, csv_file, fieldnames)
    # cluster_data2 = read_csv(csv_file)


if __name__ == "__main__":
    main()
