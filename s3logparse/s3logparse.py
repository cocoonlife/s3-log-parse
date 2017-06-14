from itertools import takewhile
from datetime import datetime
import csv


def raw_fields(line):
    """
    Iterate through the raw text of each field in a log line
    """
    line_chars = (c for c in line)
    while True:
        try:
            first_char = next(line_chars)
        except StopIteration:
            break
        if first_char == '[':
            yield ''.join(list(takewhile(lambda c: c != ']', line_chars)))
            next(line_chars)
        elif first_char == '"':
            yield ''.join(list(takewhile(lambda c: c != '"', line_chars)))
            next(line_chars)
        else:
            yield ''.join(
                [first_char] + list(takewhile(lambda c: c != ' ', line_chars))
            )


def _row_inflators():
    # return a list of functions which can each be used to modify each element
    # of a log line
    def str_inflator(s):
        return None if s == '-' else s

    def int_inflator(i):
        return 0 if i == '-' else int(i)

    def dt_inflator(d):
        return datetime.strptime(d, '%d/%b/%Y:%H:%M:%S %z')

    # most of the 18 cols are strings so prefil with str inflator
    inflators = [str_inflator] * 18
    # third column is a date
    inflators[2] = dt_inflator
    # numeric fields, all integers
    for i in [9, 11, 12, 13, 14]:
        inflators[i] = int_inflator
    return inflators


def get_line_parser():
    """
    Return a function that can parse a single log line and return a tuple of
    log line elements of the correct type
    """
    inflators = _row_inflators()

    def consume_line(line):
        # define a generator that inflates each field
        field_iter = (inflators[i](f) for i, f in enumerate(raw_fields(line)))
        # return a tuple that exhausts the genrator
        return tuple(field_iter)

    return consume_line


def tsv_outputter(output_stream):
    """
    Return a function that serializes a tuple of log line fields
    """
    csv_writer = csv.writer(output_stream, dialect=csv.excel_tab)

    def write_output(log_fields):
        # convert datetime field to iso format
        fields = list(log_fields)
        fields[2] = fields[2].isoformat()
        csv_writer.writerow(fields)

    return write_output
