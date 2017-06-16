from itertools import takewhile, chain
from datetime import datetime
from collections import namedtuple
import pytz


LogLine = namedtuple('LogLine', [
    'bucket_owner', 'bucket', 'timestamp', 'remote_ip', 'requester',
    'request_id', 'operation', 's3_key', 'request_uri', 'status_code',
    'error_code', 'bytes_sent', 'object_size', 'total_time',
    'turn_around_time', 'referrer', 'user_agent', 'version_id'
])


def raw_fields(line):
    """
    Iterate through the raw text of each field in a log line
    """
    line_chars = (c for c in line)
    for first_char in line_chars:
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
    """
    Process n string fields and convert to None if they're empty
    """
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
        try:
            yield datetime.strptime(d, '%d/%b/%Y:%H:%M:%S %z')
        except ValueError:
            # python2 doesn't understand %z modifier
            # crude fallback
            dt = datetime.strptime(d, '%d/%b/%Y:%H:%M:%S +0000')
            yield dt.replace(tzinfo=pytz.utc)


def parse_to_tuples(line_iter):
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


def parse_log_lines(line_iter):
    """
    Parse log lines from an iterable and return LogLine objects with
    appropriate accessors
    """
    for line_tuple in parse_to_tuples(line_iter):
        yield LogLine(*line_tuple)
