"""
Usage:
    mgf2csv.py [-vrh -t <data_type>] (-i <mgf_file>) [--projectid <projectid>]

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
    mgf2csv.py -i test.mgf
    mgf2csv.py -i test.mgf -t peak
"""

import csv
from pyteomics import mgf
from docopt import docopt


def get_info(spectrum, data_type):
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

def get_row(spectrum, data_type):
    spectrumTitle, precursorMz, precursorIntens, charge, seq, mods, peaklistMz, peaklistIntens = get_info(spectrum, data_type)
    peaklistMz = ",".join('%s' %id for id in peaklistMz)
    peaklistIntens = ",".join('%s' %id for id in peaklistIntens)
    spec_list = []
    psm_list = []
    spec_list.append(spectrumTitle)
    spec_list.append(precursorMz)
    spec_list.append(precursorIntens)
    spec_list.append(charge)
    spec_list.append(peaklistMz)
    spec_list.append(peaklistIntens)

    if data_type == "peak_psm" and None != seq:
        psm_list.append(spectrumTitle)
        psm_list.append(seq)
        psm_list.append(mods)

    return spec_list, psm_list 

def write_to_csv (mgf_file, data_type):
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
    for spectrum in spectra_list:
        (spec_list,psm_list) = get_row(spectrum, data_type)
        spec_writer.writerow(spec_list)
        if data_type == "peak_psm":
            psm_writer.writerow(psm_list)
    print("The data had been wrote in the csv file.")

if __name__ == '__main__':
    # arguments = docopt(__doc__)
    arguments = docopt(__doc__, version='mgf2csv 0.1')
    mgf_file = arguments['--inputfile']
    data_type = arguments.get('--type', 'peak')
    write_to_csv(mgf_file, data_type)
