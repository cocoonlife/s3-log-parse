from itertools import takewhile, chain
from datetime import datetime


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
            yield ''.join(takewhile(lambda c: c != ']', line_chars))
            next(line_chars)
        elif first_char == '"':
            yield ''.join(takewhile(lambda c: c != '"', line_chars))
            next(line_chars)
        else:
            yield first_char + ''.join(
                list(takewhile(lambda c: c != ' ', line_chars))
            )


def shift_string_fields(fields, n):
    for _ in range(n):
        s = next(fields)
        yield None if s == '-' else s


def shift_int_fields(fields, n):
    for _ in range(n):
        i = next(fields)
        # for numeric fields "-" is used for 0
        yield 0 if i == '-' else int(i)


def shift_date_fields(fields, n):
    for _ in range(n):
        d = next(fields)
        yield datetime.strptime(d, '%d/%b/%Y:%H:%M:%S %z')


def parse_lines(line_iter):
    """
    Accept an iterator that provides log lines and return an iterator which
    gives back tuples of log fields in the appropriate python datatype
    """
    # define a generator that inflates each field
    for line in line_iter:
        field_iter = raw_fields(line.rstrip())
        # unpack each field into appropriate data type
        row = tuple(chain.from_iterable([
            shift_string_fields(field_iter, 2),
            shift_date_fields(field_iter, 1),
            shift_string_fields(field_iter, 6),
            shift_int_fields(field_iter, 1),
            shift_string_fields(field_iter, 1),
            shift_int_fields(field_iter, 4),
            shift_string_fields(field_iter, 3)
        ]))
        yield row
