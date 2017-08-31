#!/usr/bin/python3
""" retrieve_splib_result.py

This tool get the target spectra and search hits from the spectra library. 
The library is built from the PRIDE Cluster consensus spectra without identified information to use.

Usage:
  retrieve_splib_result.py --input=<results.pep.xml> [--output=<output_file>]
  retrieve_splib_result.py (--help | --version)

Options:
  -i, --input=<results.pep.xml>        Path to the result .pep.xml file to process, could be a directory or pep.xml file
  --output =[output file name]         Output file for storing the spectra library search result in tabular format.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.

"""
import sys
import os
from docopt import docopt
import xml.etree.ElementTree as ET

def get_lib_spec_id(protein_str):
    words = protein_str.split("_") 
    return words[1]

def remove_pepxml_ext(path_name):
    return path_name[:-7]

def get_spec_title(mzML_path):
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
                id_map[index] = id_value
                if id_value == None:
                    raise("Spectrum index" + index + " has empty spectrum title")
    mzMLfile.close()
    return(id_map)

def write_to_file(output_file, search_results):
    """    
    "spec_title varchar(100) NOT NULL,"    + \
    "scan_in_lib int(15) COLLATE utf8_bin NOT NULL,"    + \
    "dot float NOT NULL,"    + \
    "delta float NOT NULL,"    + \
    "dot_bias float NOT NULL,"    + \
    "mz_diff float NOT NULL,"    + \
    "fval float NOT NULL,"    + \
    """

    with open(output_file, 'a') as o:
        for spec_title in search_results.keys():
            search_result = search_results.get(spec_title)
            scan_in_lib = search_result.get('lib_spec_id')
            dot = search_result.get('dot')
            fval = search_result.get('fval')
            o.write("%s\t%s\t%s\t%s\n"%(spec_title, scan_in_lib, dot, fval))

def retrieve_file(output_file, pepxml_path, title_map):
    ns_str = "{http://regis-web.systemsbiology.net/pepXML}"
    tree = ET.parse(pepxml_path)
    root = tree.getroot()

    print("Starting to retrieve" + pepxml_path)
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
    write_to_file(output_file, search_results)
    print("Retrieving of " + pepxml_path + "is done.")
    print("Totally " + str(count) + "spectra have been imported.")

def write_head_to_file(output_file):
    with open(output_file, 'w') as o:
        o.write("%s\t%s\t%s\t%s\n"%('spec_title', 'spec_in_lib', 'dot', 'fval'))

def process(input_path, output_file):
    write_head_to_file(output_file)
    if os.path.isfile(input_path):
        mzML_path = remove_pepxml_ext(input_path) + "mzML"
        title_map = get_spec_title(mzML_path)
        retrieve_file(output_file, input_path, title_map)
    else:
    	for file in os.listdir(input_path):
            if not file.lower().endswith('.pep.xml'):
                continue
            mzML_path = input_path + "/" + remove_pepxml_ext(file) + "mzML"
            title_map = get_spec_title(mzML_path)
            retrieve_file(output_file, input_path + "/" + file, title_map)

def main():
    arguments = docopt(__doc__, version='retrieve_splib_result.py 1.0 BETA')
    input_path = arguments['--input'] or arguments['-i']
    
    table_name = "test_spec_lib_search_result_1"
    
    output_file = 'lib_search_result.tab'
    if arguments['--output']:
        output_file = arguments['--output']
    
    process(input_path, output_file)


if __name__ == "__main__":
    main()
