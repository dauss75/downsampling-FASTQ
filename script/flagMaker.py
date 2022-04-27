#!/isilon/prodx/bcbio/anaconda/bin/python

import os
import argparse

def submit_flag(flag_folder, flag_file):
    fname=flag_folder + "/" + flag_file + ".done"
    os.system('touch {}'.format(fname))
    
def __main__():

    parser = argparse.ArgumentParser()
    parser.add_argument('--flag_folder', dest="flag_folder", help="flag_folder", required=True)
    parser.add_argument('--flag_file', dest="flag_file", help="flag_file", required=True)
    args = parser.parse_args()

    submit_flag(args.flag_folder, args.flag_file)

if __name__=="__main__": __main__()




