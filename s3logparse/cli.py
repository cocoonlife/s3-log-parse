import argparse
from . import s3logparse
import sys


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

    line_parser = s3logparse.get_line_parser()
    outputter = s3logparse.tsv_outputter(sys.stdout)

    for line in args.infile.readlines():
        outputter(line_parser(line.rstrip()))
