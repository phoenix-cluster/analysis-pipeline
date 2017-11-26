#!/usr/bin/python3
""" identi_data_to_file.py

This tool retrive the target spectra's identification data into the output file. 

Usage:
  identi_data_to_file.py --input=<path_to_mgf_files> [--output=<output_file>] 
  identi_data_to_file.py (--help | --version)

Options:
  -i, --input=<path>        	       Path to the mgf files to process.
  -o, --output =[output file name]            Tabular file which stores the spectra library search result.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.

"""
import sys
import os,glob
from docopt import docopt
import xml.etree.ElementTree as ET
from pyteomics import mgf
import phoenix_import_util as phoenix_writer

def write_to_file(identifications, output_file):
    with open(output_file,'w') as o:
        o.write("spectrum_title\tsequence\n")
        for spec_title in identifications.keys():
            o.write("%s\t%s\n"%(spec_title, identifications[spec_title]))
"""
process the identification data, persisit them in file or phoenix_db
@deprected, the identified data has been already in phoenix, and the mgf files nolonger has the identification infomation
"""
def process(mgf_path, output_file):

    identifications = dict()
    imported_n = 0
    for file in os.listdir(mgf_path):
        if not file.lower().endswith('.mgf'):
            continue
        imported_n +=1
        for spectrum in mgf.read(mgf_path + "/" + file):
            params = spectrum.get('params')
            title = params.get('title')
            seq = params.get('seq')
            if seq == "" or seq == None:
#                seq = "_UNID_"
                continue
            identifications[title] = seq            
    if imported_n < 1:  #read from old identification files
        if os.path.isfile(output_file) and os.path.getsize(output_file)>1000:
            with open(output_file,'r') as o:
                lines = o.readlines()[1:]  #remove the table head line
            for line in lines:
                line = line.rstrip()
                sections = line.split("\t")
                title = sections[0]
                seq = sections[1]
                identifications[title] = seq            
        else:
            raise Exception("No mgf file found here! %s"%(mgf_path))
    write_to_file(identifications, output_file)
    #phoenix_writer.export_ident_to_phoenix("pxd000021_test", "localhost", identifications)


    """
    'params': {'seq': 'FEDSLCK', 'charge': [2], 'taxonomy': '9606', 'user03': '4-MOD:00696,6-MOD:01090', 'title': 'id=PXD000021;PRIDE_Exp_Complete_Ac_27184.xml;spectrum=4970', 'pepmass': (489.68381, None)}
    """
def main():
    arguments = docopt(__doc__, version='identi_data_to_file.py 1.0 BETA')
    mgf_path = arguments['--input'] or arguments['-i']
    output_file = 'identified_spectra.tab'
    if arguments['--output']:
        output_file = arguments['--output'] or arguments['-o']
    process(mgf_path, output_file)

if __name__ == "__main__":
    main()
