from __future__ import print_function
import os
import pandas as pd
import fastavro
from utils import generate_record, schema

# import argparse
# import logging

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s %(levelname)s %(message)s')
# logger = logging.getLogger(__file__)

OUTPUT_DIR = 'output_dir'
OUT_FILE_AVRO = 'output.avro'
OUT_FILE_CSV = 'output.csv'


def etl(
        indir=(r'C:\Users\cpelosi\Downloads\test_per_data_engineer\test_per_data_engineer\data\in_text'),
        output_avro_file=(r'test_per_data_engineer\output_dir\output.avro'),
        output_csv_file=(r'test_per_data_engineer\output_dir\output.csv')
    ):
    record_list = []

    
    for file_name in os.listdir(indir):
        if file_name.endswith('.txt'):
            file_path = os.path.join(indir, file_name)
            record = generate_record(file_path)
            record_list.append(record)
    
    with open(output_avro_file, 'wb') as out_avro:
        fastavro.writer(out_avro, schema, record_list)

    # df = pd.DataFrame(record_list)
    # df.to_csv(output_csv_file, index=False)

    pass



if __name__ == '__main__':

    etl()

    # argparser = argparse.ArgumentParser(add_help=True)
    # argparser.add_argument('-i', '--indir', type=str, help=(
    #     'input dir, with files .txt, plain text, one for each document'), required=True)
    # argparser.add_argument('--outfile_avro', type=str,
    #                        default=OUT_FILE_AVRO, help=('out file avro. Default:%s'%OUT_FILE_AVRO))
    # argparser.add_argument('--outfile_csv', type=str,
    #                        default=OUT_FILE_CSV, help=('out file csv. Default:%s'%OUT_FILE_CSV))
    # args = argparser.parse_args()
    # indir = args.indir
    # outfile_avro = args.outfile_avro
    # outfile_csv = args.outfile_csv
    # logger.info("indir:%s, outfile_avro:%s, outfile_csv:%s" %
    #             (indir, outfile_avro, outfile_csv))
    # etl(indir, output_avro_file=outfile_avro, output_csv_file=outfile_csv)
    # logger.info("done")
