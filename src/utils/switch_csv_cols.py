""" reorder_csv_cols.py

This tool reorder the columns of

Usage:
  reorder_csv_cols.py --csv=<csv_file>  --order=<order_in_letters>
  reorder_csv_cols.py (--help | --version )

Options:
  -c, --csv=<csv file>      csv file to be reordered.
  -o, --order=<order in letters>   CBDA like string, the fourth col will be the first col in the reordered file.
  -h, --help                       Print this help message.
  -v, --version                    Print the current version.

order_inLetters: CBA(invert), DEBAC,...

"""
import pandas as pd
from docopt import docopt
import os

arguments = docopt(__doc__, version='switch_csv_cols v 0.0.1')
csv_file = arguments['--csv'] or arguments['-c']
order = arguments['--order'] or arguments['-o']
order_char_list = list(order)

df = pd.read_csv(csv_file, header=None)
df.columns = order_char_list
os.rename(csv_file, csv_file+".bak")

#do the reorder
order_char_list.sort()
df_reorder = df[order_char_list] # rearrange column here

df_reorder.to_csv(csv_file, header=False, index=False)






