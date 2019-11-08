"""
Usage:
    mgf2csv.py [-vrh -t <data_type>] (-i <mgf_file> --projectid <projectid>)

Arguments:
    mgf_file            Required input mgf file
    data_type           data type in input mgf file: peak/peak_psm

Options:
    -h --help                                           show this
    -v                                                  verbose mode
    -q, --quite Sel                                     quite mode
    -r                                                  make report
    -i <mgf_file>,--inputfile <mgf_file>                input mgf file
    -p,--projectid <project_id>,                        project id
    -t <data_type>,--type <file_type>                   input mgf file type, default is peak

Example
    mgf2csv.py -i test_old.mgf
    mgf2csv.py -i test_old.mgf -t peak
"""

import csv
from pyteomics import mgf
from docopt import docopt
import os


def get_spec_info(spectrum, data_type):
    """
    get spectrum data
    :param spectrum:
    :param data_type: peak vs peak_psm
    :return:
    """
    params = spectrum.get('params')
    title = params.get('title')
    spectrumTitle = title
    # id = spectrumTitle[:9]
    precursorMz = params.get('pepmass')[0]
    # if list(params)[2] not in "title,pepmass,charge,taxonomy,seq,user03,m/z array,intensity array":
    #     precursorIntens = params.get(list(params)[2])
    # else :
    precursorIntens = "0"
    charge = params.get('charge')
    peaklistMz = spectrum.get('m/z array')
    peaklistIntens = spectrum.get('intensity array')
    if data_type == "peak_psm":
        seq = params.get('seq')
        mods = params.get('mods')
    else:
        seq = None
        mods = None

    cleanPeaklistMz = list()
    cleanPeaklistIntens = list()
    for peakMz,peakIntens in zip(peaklistMz, peaklistIntens):
        if float(peakIntens) == 0:
            continue
        else:
            cleanPeaklistMz.append(peakMz)
            cleanPeaklistIntens.append(peakIntens)

    return spectrumTitle,precursorMz,precursorIntens,charge, seq, mods, cleanPeaklistMz,cleanPeaklistIntens

def get_row(projectid, filename, index, spectrum, data_type):
    """
    get row data for a spectrum
    :param spectrum:
    :param data_type: peak vs peak_psm
    :return:
    """
    spectrumTitle, precursorMz, precursorIntens, charge, seq, mods, peaklistMz, peaklistIntens = get_spec_info(spectrum, data_type)
    spectrumTitle = "%s;%s;spectrum=%d"%(projectid, filename, index) #biuld new title with the projectid;filename;index
    peaklistMz = ",".join('%s' %id for id in peaklistMz)
    peaklistIntens = ",".join('%s' %id for id in peaklistIntens)
    spec_row = []
    psm_row = []
    spec_row.append(spectrumTitle)
    spec_row.append(precursorMz)
    spec_row.append(precursorIntens)
    spec_row.append(charge)
    spec_row.append(peaklistMz)
    spec_row.append(peaklistIntens)

    if data_type == "peak_psm" and None != seq:
        psm_row.append(spectrumTitle)
        psm_row.append(seq)
        psm_row.append(mods)

    return spec_row, psm_row

def write_to_csv (projectid, mgf_file, data_type):

    filename = os.path.basename(mgf_file)

    spec_file_name = mgf_file[:-4] + "_spec.csv"
    spec_file = open(spec_file_name,"w")
    spec_writer = csv.writer(spec_file)
    spec_writer.writerow(['spectrumTitle','precursorMz','precursorIntens','charge','peaklistMz','peaklistIntens'])

    if data_type == "peak_psm":
        psm_file_name = mgf_file[:-4] + "_psm.csv"
        psm_file = open(psm_file_name,"w")
        psm_writer = csv.writer(psm_file)
        psm_writer.writerow(['spectrumTitle','sequence','modifications'])

    spectra_list = mgf.read(mgf_file)

    print("Handling the data in %s"%(mgf_file))
    for index, spectrum in enumerate(spectra_list, start=1):  # default is zero
        (spec_row,psm_row) = get_row(projectid, filename, index, spectrum, data_type)
        spec_writer.writerow(spec_row)
        if data_type == "peak_psm":
            psm_writer.writerow(psm_row)
    print("The data had been wrote in the csv file.")

if __name__ == '__main__':
    # arguments = docopt(__doc__)
    arguments = docopt(__doc__, version='mgf2csv 0.1')
    mgf_file = arguments['--inputfile']
    data_type = arguments.get('--type', 'peak')
    projectid = arguments.get('--projectid', None) or arguments.get("-p", None)
    if not projectid:
        raise Exception("Error, no project id input.")
    write_to_csv(projectid,mgf_file, data_type)
