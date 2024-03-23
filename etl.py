#!/usr/bin/python
# -*- coding: utf-8 -*-
#

""" 
Suggerito python3 con pandas e fastavro

Esempio:
  $ python etl.py -i data/in_text/


usage: etl.py [-h] -i INDIR

optional arguments:
  -h, --help            show this help message and exit
  -i INDIR, --indir INDIR
                        input dir, with files .txt, plain text, one for each
                        document
  --outfile_avro OUTFILE_AVRO
                        out file avro. Default:output.avro
  --outfile_csv OUTFILE_CSV
                        out file csv. Default:output.csv

"""

from __future__ import print_function
import os
import pandas as pd
import fastavro
from utils import generate_record, schema

import argparse
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__file__)

OUTPUT_DIR = 'output_dir'
OUT_FILE_AVRO = 'output.avro'
OUT_FILE_CSV = 'output.csv'


def etl(indir, output_avro_file, output_csv_file):
    record_list = []

    try:
        for file_name in os.listdir(indir):
            if file_name.endswith('.txt'):
                file_path = os.path.join(indir, file_name)
                record = generate_record(file_path)
                record_list.append(record)
        
        with open(output_avro_file, 'wb') as out_avro:
            fastavro.writer(out_avro, schema, record_list)

        df = pd.DataFrame(record_list)
        df.to_csv(output_csv_file, index=False)

        logger.info("ETL successfully completed!")
    
    except Exception as e:
        logger.error("An error occurred during the ETL: %s", str(e))


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument('-i', '--indir', type=str, help=(
        'input dir, with files .txt, plain text, one for each document'), required=True)
    argparser.add_argument('--outfile_avro', type=str,
                           default=OUT_FILE_AVRO, help=('out file avro. Default:%s'%OUT_FILE_AVRO))
    argparser.add_argument('--outfile_csv', type=str,
                           default=OUT_FILE_CSV, help=('out file csv. Default:%s'%OUT_FILE_CSV))
    args = argparser.parse_args()
    indir = args.indir
    outfile_avro = args.outfile_avro
    outfile_csv = args.outfile_csv
    logger.info("indir:%s, outfile_avro:%s, outfile_csv:%s" %
                (indir, outfile_avro, outfile_csv))
    etl(indir, output_avro_file=outfile_avro, output_csv_file=outfile_csv)
    logger.info("done")
