"""analyze_results.py

This tool analyze the import results from PROJECT_spec_match_details.csv

Usage:
  analyze_results.py --file=<PROJECT_spec_match_details.csv>
  analyze_results.py (--help | --version)

Options:
  -f, --file =<PROJECT_spec_match_details>        csv file to be processed.
  -h, --help                       Print this help message.
  -v, --version                    Print the current version.

"""

import os
import csv
import logging
from docopt import docopt

def read_csv(csv_file):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) < 1:
        return None
    with open(csv_file, 'r') as f:
        new_dict = {}
        reader = csv.reader(f, delimiter=',')
        fieldnames_from_file = next(reader)
        # if str(fieldnames_from_file) != str(fieldnames):
        #     raise Exception("the fields name not matched: " + str(fieldnames) + " vs. " + str(fieldnames_from_file))

        reader = csv.DictReader(f, fieldnames=fieldnames_from_file, delimiter=',')
        for row in reader:
            spec_title = row.pop('spec_title')
            new_dict[spec_title] = row
    return new_dict
    # logging.info("Read %d lines from spectra library search result file %s"%(len(new_dict), csv_file))

def main():
    arguments = docopt(__doc__, version='analyze_results.py 1.0 BETA')
    file_name = arguments['--file'] or arguments['-f']
    match_result_details = read_csv(file_name)
    print(len(match_result_details))

if __name__ == "__main__":
    main()
