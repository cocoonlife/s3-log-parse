import argparse
from . import s3logparse
import sys
import csv


def main():
    """
    Command line tool for processing S3 access logs
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        'infile',
        nargs='?',
        type=argparse.FileType('r'),
        default=sys.stdin
    )
    args = argparser.parse_args()

    tsv_writer = csv.writer(sys.stdout, dialect=csv.excel_tab)
    tsv_writer.writerows(s3logparse.parse_to_tuples(args.infile.readlines()))
