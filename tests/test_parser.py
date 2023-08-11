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

    def test_date_string(self):
        """
        Test that a date delimited by square brakets is correctly handled
        """
        log_line = 'hello [06/Feb/2014:00:00:38 +0000] world'
        self.assertEqual(
            list(s3logparse.raw_fields(log_line))[1],
            '06/Feb/2014:00:00:38 +0000'
        )


class LineParserTupleTest(TestCase):
    """
    Test parsing a single line with the raw tuple parse works ok
    """

    def setUp(self):
        log_stream = s3logparse.parse_to_tuples([
            '79a59df900b949e55d96a1e698fbacedfd6e09d9\
8eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2014:00:00:38 +0000] 192.0.2.3 79a59\
df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE \
REST.GET.VERSIONING - "GET /mybucket?versioning HTTP/1.1" 200 - 113 - 7 - "-" \
"S3Console/0.4" - EXAfPIQ4LEOWDMQM+ey7A9XgZhWnQ2JMAXIFOURb7hASDFGH+Jd1vEXPLEAMa3Km= \
SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader bucket-name.s3.amazonaws.com TLSv1.2 - -'
        ])
        self.log_fields = next(log_stream)

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
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
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
            [1, 1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0]
        )
        for s in str_fields:
            self.assertIsInstance(s, str)

    def test_null_fields(self):
        """
        Test that null string fields are correctly represented by None
        """
        none_fields = list(itertools.compress(
            self.log_fields,
            [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1]
        ))
        self.assertEqual(none_fields, [None] * 6)


class LogLineParserTests(TestCase):

    def test_attributes(self):
        """
        The attributes of the LogLine named tuple are as expected
        """
        log_stream = s3logparse.parse_log_lines([
            '79a59df900b949e55d96a1e698fbacedfd6e09d9\
8eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2014:00:00:38 +0000] 192.0.2.3 79a59\
df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE \
REST.GET.VERSIONING - "GET /mybucket?versioning HTTP/1.1" 200 - 113 - 7 - "-" \
"S3Console/0.4" - EXAfPIQ4LEOWDMQM+ey7A9XgZhWnQ2JMAXIFOURb7hASDFGH+Jd1vEXPLEAMa3Km= \
SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader bucket-name.s3.amazonaws.com TLSv1.2 - -'
        ])
        log_line = next(log_stream)
        self.assertEqual(
            log_line.bucket_owner,
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be'
        )
        self.assertEqual(
            log_line.bucket,
            'mybucket'
        )
        self.assertEqual(
            log_line.timestamp.isoformat(),
            '2014-02-06T00:00:38+00:00'
        )
        self.assertEqual(
            log_line.remote_ip,
            '192.0.2.3'
        )
        self.assertEqual(
            log_line.requester,
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be'
        )
        self.assertEqual(
            log_line.request_id,
            '3E57427F3EXAMPLE'
        )
        self.assertEqual(
            log_line.operation,
            'REST.GET.VERSIONING'
        )
        self.assertEqual(
            log_line.s3_key,
            None
        )
        self.assertEqual(
            log_line.request_uri,
            'GET /mybucket?versioning HTTP/1.1'
        )
        self.assertEqual(
            log_line.status_code,
            200
        )
        self.assertEqual(
            log_line.error_code,
            None
        )
        self.assertEqual(
            log_line.bytes_sent,
            113
        )
        self.assertEqual(
            log_line.object_size,
            0
        )
        self.assertEqual(
            log_line.total_time,
            7
        )
        self.assertEqual(
            log_line.turn_around_time,
            0
        )
        self.assertEqual(
            log_line.referrer,
            None
        )
        self.assertEqual(
            log_line.user_agent,
            "S3Console/0.4"
        )
        self.assertEqual(
            log_line.version_id,
            None
        )
        self.assertEqual(
            log_line.host_id,
            'EXAfPIQ4LEOWDMQM+ey7A9XgZhWnQ2JMAXIFOURb7hASDFGH+Jd1vEXPLEAMa3Km='
        )
        self.assertEqual(
            log_line.signature_version,
            'SigV4'
        )
        self.assertEqual(
            log_line.cipher_suite,
            'ECDHE-RSA-AES128-GCM-SHA256'
        )
        self.assertEqual(
            log_line.authentication_type,
            'AuthHeader'
        )
        self.assertEqual(
            log_line.host_header,
            'bucket-name.s3.amazonaws.com'
        )
        self.assertEqual(
            log_line.tls_version,
            'TLSv1.2'
        )
        self.assertEqual(
            log_line.access_point_arn,
            None
        )
        self.assertEqual(
            log_line.acl_required,
            None
        )
