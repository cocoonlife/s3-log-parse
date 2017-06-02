from unittest import TestCase
from s3logparse import s3logparse
from datetime import datetime
import itertools

class FieldSeparatorTests(TestCase):
    """
    Tests to validate the early stage of the processing pipeline which
    separates fields from the log line into strings
    """

    def test_unquoted_strings(self):
        """
        Check that unquoted strings are handled OK
        """
        log_line = 'hello world'
        self.assertEqual(
            list(s3logparse.raw_fields(log_line)),
            ['hello', 'world']
        )

    def test_quoted_string(self):
        """
        Check that unquoted strings are handled OK
        """
        log_line = 'hello "hello world" world'
        self.assertEqual(
            list(s3logparse.raw_fields(log_line))[1],
            'hello world'
        )

    def test_escaped_quoted_string(self):
        """
        Check that escaped quotes are handled OK
        """
        log_line = r'hello "hello \"world\"" world'
        self.assertEqual(
            list(s3logparse.raw_fields(log_line))[1],
            'hello "world"'
        )

    def test_date_string(self):
        """
        Test that a date delimited by square brakets is correctly handled
        """
        log_line = 'hello [06/Feb/2014:00:00:38 +0000] world'
        self.assertEqual(
            list(s3logparse.raw_fields(log_line))[1],
            '06/Feb/2014:00:00:38 +0000'
        )


class LineParserTest(TestCase):
    """
    Test parsing a single line works OK
    """

    def setUp(self):
        log_parser = s3logparse.get_line_parser()
        self.log_fields = log_parser('79a59df900b949e55d96a1e698fbacedfd6e09d9\
8eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2014:00:00:38 +0000] 192.0.2.3 79a59\
df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE \
REST.GET.VERSIONING - "GET /mybucket?versioning HTTP/1.1" 200 - 113 - 7 - "-" \
"S3Console/0.4" -')

    def test_date_field(self):
        """
        Test that the date field is a datetime obj
        """
        self.assertIsInstance(
            self.log_fields[2],
            datetime
        )
        # check reversing formatting works ok
        self.assertEqual(
            self.log_fields[2].strftime('%d/%b/%Y:%H:%M:%S %z'),
            '06/Feb/2014:00:00:38 +0000'
        )

    def test_int_fields(self):
        """
        Test integer fields are correctly handled
        """
        int_fields = list(itertools.compress(
            self.log_fields,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 0]
        ))
        self.assertEqual(
            int_fields,
            [200, 113, 0, 7, 0]
        )

    def test_str_fields(self):
        """
        Test that string fields are strings
        """
        str_fields = itertools.compress(
            self.log_fields,
            [1, 1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0]
        )
        for s in str_fields:
            self.assertIsInstance(s, str)

    def test_null_fields(self):
        """
        Test that null string fields are correctly represented by None
        """
        none_fields = list(itertools.compress(
            self.log_fields,
            [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1]
        ))
        self.assertEqual(none_fields, [None] * 4)
