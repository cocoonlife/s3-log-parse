==========
s3logparse
==========

A simple tool for parsing AWS S3 access logs. Can either be used from Python or via a command line wrapper. 

Installation
------------

Download this repo and run

.. code:: console

  python3 setup.py install


Command Line usage
------------------

The s3logparse command line script will reformat logs according to the following conventions

- All fields are tab separated
- No fields are quoted
- Null text fields (represented by -) are empty
- Timestamp is ISO8601 formatted
- Null numeric fields are converted to 0

The script can accept a single argument, the name of a log file

.. code:: console

  s3logparse mys3logfile.txt

piping will also work

.. code:: console

  cat allmys3logs/* | s3logparse


Python API
----------

The parse_log_lines function iterates over raw log lines and converts them to a namedtuple with the following attributes: bucket_owner, bucket, timestamp, remote_ip, requester, request_id, operation, s3_key, request_uri, status_code, error_code, bytes_sent, object_size, total_time, turn_around_time, referrer, user_agent, version_id

Here is a simple example which extracts IP addresses from logs

.. code:: python

  from s3logparse import s3logparse

  with open('mys3logs.txt') as fh:
      for log_entry in parse_log_lines(fh.readlines()):
          print(log_entry.ip_address)


