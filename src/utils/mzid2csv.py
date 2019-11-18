#!/usr/bin/python3
"""
This program retrieve PSMs from the mzident ml file, write them to csv file

Usage:
mzid2csv.py --input <input_file>
[--peakfile <peak_file>]
[--projectid <project_id>]
[--output <output_path>]
[--score_field <score_field>]
[--title_field <title_field>]
[--fdr <fdr>]
[--decoy_string <decoy_string>]
[--larger_score_is_better]
[--include_decoy]
mzid2csv.py (--help | --version)

Options:
-i, --input=<inputfile>             The path to the mzIdentML file
-p, --projectid=<inputfile>             The path to the mzIdentML file
--peakfile=<peakfile>               The path to the related peak file
-o, --output=<output_path>          The output path to write to csv, the output file name should be as same as input
--score_field=<score_field>         The name of the score's field (**Important**: do not supply the accession
                                    but only the name)
--title_field=<title_field>         The name of the field supplying the spectrum's title (in SpectrumIdentificationResult).
--fdr=<fdr>                         Target FDR (default 2). If set to "2" the original cut-off is used.
--larger_score_is_better            Logical indicating whether better scores mean a more reliable
                                    result. Default is False as most search engines report probabilities
--include_decoy                     If set to True decoy hits are also returned.
--decoy_string=<decoy_string>       String used to identify decoy proteins, default is DECOY.

-h, --help                           Print this help message.
-v, --version                        Print the current version.
"""


import sys, os
# sys.path.insert(0, "../py-venv/lib/python3.6/site-packages")
# sys.path.insert(0, "/code/py-venv/lib/python3.6/site-packages")
from pyteomics import mgf
import logging
import time
from docopt import docopt
import csv

parent_path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, parent_path)
import  utils.mzident_reader as mzident_reader

def mzid2csv(projectid, filename, peak_file, outfile, score_field, title_field, fdr, decoy_string,
             larger_score_is_better, include_decoy):

    # score_field = "Scaffold:Peptide Probability"

    psms = mzident_reader.parser_mzident2(filename=filename, score_field=score_field,
                                          title_field=title_field, fdr=fdr, decoy_string=decoy_string,
                                          larger_score_is_better=larger_score_is_better,
                                          include_decoy=include_decoy)
    with open(outfile, "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Title", "Sequence", "Modification", "Charge", "PrecursorMz"])
        for psm in psms:
            spec_title = "%s;%s;spectrum=%d"%(projectid, os.path.basename(peak_file), psm["index"])
            writer.writerow([spec_title, psm['sequence'], psm.get("ptms", ''), psm.get("charge"), psm.get("prec_mz")])


def main():

    arguments = docopt(__doc__, version='mzid2csv 0.0.1')
    mzid_file = arguments.get('--input') or arguments['-i']
    projectid = arguments.get('--projectid') or arguments['-p']
    peak_file = arguments.get('--peakfile', None)
    score_field = arguments.get('score_field', None)
    out_path = arguments.get('--output', None)

    if not out_path:
        out_path = arguments.get('-o', '.')
    if not score_field or not peak_file:
        (tmp_score_field, tmp_peak_file) = mzident_reader.get_scfield_peakfile(mzid_file)
        if not score_field:
            score_field = tmp_score_field
        if not peak_file:
            peak_file = tmp_peak_file
    outfile = os.path.join(out_path, os.path.splitext(mzid_file)[0]+"_psm.csv")

    if not os.path.exists(peak_file):
        raise Exception("source peak file %s does not exist for mzid file %s"%(peak_file, mzid_file))

    title_field = arguments.get('title_field', None)
    fdr= arguments.get('fdr', 2)
    decoy_string = arguments.get('decoy_string', "DECOY")
    larger_score_is_bettertitle_field = arguments.get('larger_score_is_better', False)
    include_decoy = arguments.get('include_decoy', False)

    inputfile = os.path.abspath(mzid_file)
    mzid2csv(projectid, inputfile, peak_file, outfile, score_field, title_field, fdr, decoy_string,
             larger_score_is_bettertitle_field, include_decoy)

if __name__ == '__main__':
    main()
