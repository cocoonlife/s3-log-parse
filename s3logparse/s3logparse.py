from itertools import takewhile
from datetime import datetime
import csv


def _consume_unquoted_text_field(first_char, ch_iter):
    """
    Raw text field
    """
    field = first_char
    for ch in takewhile(lambda c: c != ' ', ch_iter):
        field = field + ch
    return field


def _consume_date_field(ch_iter):
    """
    Date field
    """
    field = ""
    for ch in takewhile(lambda c: c != ']', ch_iter):
        field = field + ch
    next(ch_iter)  # throw away next character
    return field


def _consume_quoted_text_field(ch_iter):
    """
    Consume a quoted text field
    """
    field = ""
    for ch in takewhile(lambda c: c != '"', ch_iter):
        if ch == '\\':
            # in the case of an escape sequence move on one character
            field = field + next(ch_iter)
        else:
            field = field + ch
    next(ch_iter)  # throw away next character
    return field


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
            # assume date is never null
            yield _consume_date_field(line_chars)
        elif first_char == '"':
            yield _consume_quoted_text_field(line_chars)
        else:
            yield _consume_unquoted_text_field(first_char, line_chars)


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
