#!/usr/bin/python
# -*- coding:utf-8 -*- 

"""
Usage:
    mzml2csv.py [-vrh] -i<input_file> -p<projectid>

Arguments:
    input_file          input mzML file
    projectid           project id

Options:
    -h --help                                       show this
    -v                                              verbose mode
    -i <input_file>,--input <input_file>            project number file
    -p <projectid>,--projectid <projectid>          project file type

Example
    mzml2csv.py -p PXD003452 -i test_old.mzml
"""

import csv, os, sys
#sys.path.insert(0, "./py-venv/lib/python3.6/site-packages")
from docopt import docopt
import pymzml


def get_row(projectid, filename, index, spectrum):
    data = []
    if spectrum['ms level'] == 2:
        list_mz = []
        list_i = []
        for mz, i in spectrum.peaks('raw'):
            list_mz.append(mz)
            list_i.append(i)
        # title = "spectrum=" + str(spectrum).split(" ")[5]
        title = "%s;%s;spectrum=%d"%(projectid, filename, index) #biuld new title with the projectid;filename;index
        data.append(title)
        data.append(spectrum["base peak m/z"])
        data.append(spectrum["base peak intensity"])
        data.append(spectrum["charge state"])
        data.append(str(list_mz).replace("[", "").replace("]", ""))
        data.append(str(list_i).replace("[", "").replace("]", ""))
        return data


def load_to_csv(projectid, file_path):
    spec_file_name = file_path[:-5] + "_spec.csv"
    msrun = pymzml.run.Reader(file_path)
    filename = os.path.basename(file_path)
    with open(spec_file_name, "a") as csvfile :
        writer = csv.writer(csvfile)
        writer.writerow(['spectrumTitle','precursorMz','precursorIntens','charge','peaklistMz','peaklistIntens'])
        for index, spectrum in enumerate(msrun, start=1):  # default is zero
            row_data = get_row(projectid, filename, index, spectrum)
            if isinstance(row_data, list):
                writer.writerow(row_data)

if __name__ == '__main__':
    arguments = docopt(__doc__)
    file_path = arguments['--input'] or arguments['-p']
    projectid = arguments['--projectid'] or arguments['-p']
    load_to_csv(projectid, file_path)






