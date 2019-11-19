#!/usr/bin/python3
""" retrieve_splib_result.py

This library get the target spectra and search hits from the spectra library, and export them to file or Phoenix/HBase table.
The library is built from the PRIDE Cluster consensus spectra without identified information to use.

"""
import os,sys
#sys.path.insert(0, "./py-venv/lib/python3.6/site-packages")
import xml.etree.ElementTree as ET
import csv,re
import logging
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import mysql_storage_access as mysql_acc
#import phoenixdb

def get_conn():
    conn = mysql_acc.get_conn()
    return conn

def get_lib_spec_id(protein_str):
    words = protein_str.split("_") 
    return words[1]

def remove_pepxml_ext(path_name):
    return path_name[:-8]


def build_spec_title(project_id, ms_run, index, value):
    enhancer_title = value
    if re.match(r'E\d{6}', project_id):
        enhancer_title = "%s;%s;spectrum=%s"%(project_id, ms_run.get('peakfile'), index)
    return enhancer_title


"""
build the map from spectrum index(in mzML) to spectrum title
"""
def get_spec_title(project_id, ms_run, mzML_path):
    mzMLfile = open(mzML_path)
    ns_str = "{http://psi.hupo.org/ms/mzml}"
    tree = ET.parse(mzMLfile)
    root = tree.getroot()
    id_map = {}
    for spectrum in root.iter(ns_str + "spectrum"):
        index = spectrum.attrib.get('index')
        for cvParam in spectrum.iterfind(ns_str + 'cvParam'):
            name = cvParam.attrib.get('name')
            if name == 'spectrum title':
                id_value = cvParam.attrib.get('value')
                enhancer_title = build_spec_title(project_id,ms_run,index,id_value)
                id_map[index] = enhancer_title
                if id_value == None:
                    raise("Spectrum index" + index + " has empty spectrum title")
    mzMLfile.close()
    return(id_map)

"""
#write the spectra library search result to csv file
"""
def write_to_csv(search_results, output_file, fieldnames):
    if len(search_results) <1:
        logging.info("ERROR, No spectra library search result to write to csv.")
        return
    with open(output_file, 'w', newline="") as f:
        w = csv.writer(f)
        w.writerow(fieldnames)
        for spec_title in search_results.keys():
            search_result = search_results.get(spec_title)
            row = []
            row.append(spec_title)
            for fieldname in fieldnames[1:]:
                row.append(search_result.get(fieldname))
            w.writerow(row)
    logging.info("Wrote %d lines to spectra library search result file %s"%(len(search_results), output_file))
"""
#read the spectra library search result from csv file
"""
def read_csv(csv_file, fieldnames):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
        return None
    with open(csv_file, 'r') as f:
        new_dict = {}
        reader = csv.reader(f, delimiter=',', skipinitialspace=True)
        fieldnames_from_file = next(reader)
        if str(fieldnames_from_file) != str(fieldnames):
            raise Exception("the fields name not matched: " + str(fieldnames) + " vs. " + str(fieldnames_from_file))

        reader = csv.DictReader(f, fieldnames=fieldnames_from_file, delimiter=',', skipinitialspace=True)
        for row in reader:
            spec_title = row.pop('spec_title')
            new_dict[spec_title] = row
    return new_dict
    logging.info("Read %d lines from spectra library search result file %s"%(len(new_dict), csv_file))


def table_is_equal_to_csv(project_id, search_result_details):
    conn = get_conn()
    cursor = conn.cursor()
    match_table_name = "T_" + project_id + "_spec_cluster_match"

    query_sql = "SELECT COUNT(*) FROM %s"%(match_table_name.upper())
    try:
        cursor.execute(query_sql)
        n_matches_in_db = cursor.fetchone()[0]
        if n_matches_in_db == len(search_result_details):
            logging.info("the table already has all matches to upsert, quit importing from csv to phoenix!")
            return True
        if n_matches_in_db <= len(search_result_details):
            logging.info("the table has less matches,  need to import from csv to phoenix!")
            return False
        else:
            logging.info("the table has more matches than csv, need to have a check!")
            raise Exception("the table has more matches than csv, need to have a check!")
    except Exception as e:
        logging.error("ERROR: faild to select from match_table_name ( " + str(e))
        return False
    finally:
        cursor.close()
        conn.close()



"""
retrive the search results
"""
def retrieve_file(pepxml_path, title_map):
    ns_str = "{http://regis-web.systemsbiology.net/pepXML}"
    tree = ET.parse(pepxml_path)
    root = tree.getroot()
    logging.info("Starting to retrieve" + pepxml_path)
    msms_run_summary = root[0]
    count = 0
    search_results = {}
    for spectrum_query in msms_run_summary.iterfind(ns_str + "spectrum_query"):
        #start_scan = spectrum_query.attrib.get('start_scan')
        #end_scan = spectrum_query.attrib.get('end_scan')
        #if start_scan != end_scan :
        #    raise Exception ("Some thing wrong with this spectrum_query, start_scan %d != end_scan %d"%(start_scan, end_scan))
        start_scan = spectrum_query.attrib.get('start_scan')
        scan_num_in_mzml = str(int(start_scan) - 1) #scan_num start at 1 in pep.xml, but at 0 in mzML
        spectrum = title_map.get(scan_num_in_mzml)
        if spectrum == None:
            raise Exception("Spectrum in pepxml at scan " + start_scan + " has no spectrum title")
        count = count + 1
        
        search_result = spectrum_query[0]
        if search_result.tag != ns_str + 'search_result':
            raise Exception ("Some thing wrong with this spectrum_query, search_result is not the first element, but %s"%search_result.tag)
        
        protein_str = "" 
        for search_hit in spectrum_query.iterfind(ns_str + "search_result/"+ ns_str + 'search_hit') :
            if int(search_hit.attrib.get('hit_rank')) > 1:
                raise Exception ("Some thing wrong with this spectrum_query, search_hit is more than one: %s"%spectrum_query.attrib.get('spectrum'))
            protein_str = search_hit.attrib.get("protein")
            lib_spec_id = get_lib_spec_id(protein_str)
            search_scores = {}
            search_scores['lib_spec_id'] = lib_spec_id
            for search_score in search_hit.getchildren():
                score_name = search_score.attrib.get('name')
                score_value = search_score.attrib.get('value')
                search_scores[score_name] = score_value 
        search_results[spectrum] = search_scores

    logging.info("Retrieving of " + pepxml_path + " is done.")
    logging.info("Totally " + str(count) + "spectra have been imported from this file.")
    return search_results

"""
write the table head to file
"""
def write_head_to_file(output_file):
    with open(output_file, 'w') as o:
        o.write("%s\t%s\t%s\t%s\n"%('spec_title', 'spec_in_lib', 'dot', 'fval'))

"""
retrieve the spec_cluster match results and persist them into the tab file
by a pep.xml file
"""
def deal_a_file(project_id, file_path, file_name, ms_runs):
    ms_run_name = remove_pepxml_ext(file_name)
    directory_path = os.path.dirname(file_path)
    ms_run = ms_runs.get(ms_run_name)
    mzML_path = os.path.join(directory_path, ms_run_name + ".mzML")
    search_results_of_file = list()

    title_map = get_spec_title(project_id, ms_run, mzML_path)
    try:
        search_results_of_file = retrieve_file(os.path.join(file_path, file_name), title_map)
    except Exception as error:
        logging.info("error in retrive search result file in %s"%(error))
    logging.info("retrived %d search result from .pep.xml file"%(len(search_results_of_file)))
    return search_results_of_file



"""
retrieve the spec_cluster match results and persist them into the tab file
"""
def retrive_search_result(project_id, file_path, csv_file, ms_runs):

    logging.info("start to process the spec_cluster match results, persisit them to phoenix_db and file(?)")
    search_results = {}
    fieldnames = ['spec_title', 'lib_spec_id', 'dot', 'fval']

    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 1:
        search_results = read_csv(csv_file, fieldnames)
        logging.info("read %d search result from csv file:%s "%(len(search_results), csv_file))
    else:
        if os.path.isfile(file_path):
            file_name = file_path
            search_results_of_file = deal_a_file(project_id, file_path, file_name, ms_runs)
            search_results.update(search_results_of_file)
        else:
            for file in os.listdir(file_path):
                if not file.lower().endswith('.pep.xml'):
                    continue
                search_results_of_file = deal_a_file(project_id, file_path, file, ms_runs)
                search_results.update(search_results_of_file)
            logging.info("retrived %d search result from .pep.xml files"%(len(search_results)))
            print("retrived %d search result from .pep.xml files"%(len(search_results)))

        if len(search_results) > 0:
            write_to_csv(search_results, csv_file, fieldnames)
    return search_results

# def main():
#     arguments = docopt(__doc__, version='retrieve_splib_result.py 1.0 BETA')
#     file_path = arguments['--input'] or arguments['-i']
#
#     table_name = "test_spec_lib_search_result_1"
#
#     output_file = 'lib_search_result.tab'
#     if arguments['--output']:
#         output_file = arguments['--output']
#
#     process(project_id, file_path, output_file)
#
#
# if __name__ == "__main__":
#     main()
