#!/usr/bin/env python3
from functools import partial
from io import BytesIO
import gzip
import logging
import tempfile

import click
from warcio.recordbuilder import ArcWarcRecord
from warcio.recordloader import ArcWarcRecordLoader, StatusAndHeaders, StatusAndHeadersParser
from warcio.warcwriter import WARCWriter

import lib


logger = logging.getLogger(__name__)


@click.group()
def main():
    pass


@main.command()
@click.argument('s3-in-bucket')
@click.argument('s3-out-bucket')
@click.option('-f', '--filter', type=str, default='parts/', help='Path prefix filter')
def repack_clueweb_warcs(s3_in_bucket, s3_out_bucket, filter):
    """
    Repack buggy ClueWeb WARC files to a modern WARC/1.0 format that is readable by warcio.
    """

    if s3_in_bucket == s3_out_bucket:
        raise click.UsageError('Input and output bucket must not be the same.')

    py_files = lib.create_lib_zip()
    sc = lib.get_spark_context()
    sc.addPyFile(py_files.name)

    s3 = lib.get_s3_resource()

    (sc
     .parallelize(o.key for o in s3.Bucket(s3_in_bucket).objects.filter(Prefix=filter))
     .repartition(sc.defaultParallelism)
     .foreach(partial(repack_warc, in_bucket=s3_in_bucket, out_bucket=s3_out_bucket)))


def repack_warc(obj_name, in_bucket, out_bucket):
    if not obj_name.endswith('.warc.gz'):
        return

    s3 = lib.get_s3_resource()
    with gzip.open(s3.Object(in_bucket, obj_name).get()['Body'], 'r') as in_warc:
        with tempfile.TemporaryFile() as tmp_file:
            warc_writer = WARCWriter(tmp_file)

            rec_headers = []
            rec_content = []
            rec_content_len = 0
            in_headers = True
            content_length = 0
            after_record = False
            last_header_was_uri = False

            for line in in_warc:
                if (in_headers or after_record) and line.startswith(b'WARC/'):
                    if rec_headers:
                        # Write previous record if not first
                        write_warc_record(warc_writer, rec_headers, rec_content)
                    after_record = False
                    rec_headers = [line]
                    rec_content = []
                    rec_content_len = 0
                    in_headers = True
                    continue

                if in_headers:
                    if not line.strip() and not last_header_was_uri:
                        in_headers = False
                        continue

                    h = line.split(b':', maxsplit=1)
                    if len(h) < 2:
                        # Erroneous multiline URL
                        rec_headers[-1] = rec_headers[-1].rstrip() + h[0]
                    else:
                        rec_headers.append(line)

                        # URIs are extremely buggy and can contain newlines
                        last_header_was_uri = h[0].strip().lower() == b'warc-target-uri'

                    if h[0].strip().lower() == b'content-length':
                        content_length = int(h[1])

                elif not in_headers and not after_record:
                    rec_content_len += len(line)
                    rec_content.append(line)
                    if content_length <= rec_content_len:
                        # Record is supposed to be done, but there may be more
                        # payload data before the next WARC header.
                        after_record = True

                elif after_record and not line.startswith(b'WARC/') and line.strip():
                    # Add excess payload to record content
                    rec_content.append(line)

            # Write last record
            if len(rec_headers) > 1:
                write_warc_record(warc_writer, rec_headers, rec_content)

            tmp_file.flush()
            tmp_file.seek(0)
            s3.Object(out_bucket, obj_name).put(Body=tmp_file)
            logger.info('Converted WARC {}'.format(obj_name))


class LenientStatusAndHeadersParser(StatusAndHeaders):
    def lenient_to_ascii_bytes(self, filter_func=None):
        """ Attempt to encode the headers block as ascii
            If encoding fails, call percent_encode_non_ascii_headers()
            to encode any headers per RFCs
        """
        try:
            string = self.to_str(filter_func)
            string = string.encode('ascii')
        except (UnicodeEncodeError, UnicodeDecodeError):
            self.percent_encode_non_ascii_headers()
            string = self.to_str(filter_func)
            string = string.encode('ascii', errors='ignore')

        return string + b'\r\n'


def write_warc_record(warc_writer, rec_headers, rec_content):
    # Monkey-patch `StatusAndHeaders.to_ascii_bytes` to ignore encoding errors
    StatusAndHeaders.to_ascii_bytes = LenientStatusAndHeadersParser.lenient_to_ascii_bytes

    rec_headers = StatusAndHeadersParser(
        ArcWarcRecordLoader.WARC_TYPES, verify=False).parse(BytesIO(b''.join(rec_headers)))
    # Remove Content-Length header, since we cannot trust it
    rec_headers.remove_header('Content-Length')

    rec_http_headers = []
    rec_payload = []

    in_headers = rec_headers.get_header('WARC-Type') in ArcWarcRecordLoader.HTTP_TYPES
    for line in rec_content:
        if in_headers:
            if not line.strip():
                in_headers = False
                continue

            rec_http_headers.append(line)
        else:
            rec_payload.append(line)

    if rec_http_headers:
        rec_http_headers = StatusAndHeadersParser(
            ArcWarcRecordLoader.HTTP_VERBS, verify=False).parse(BytesIO(b''.join(rec_http_headers)))
    else:
        rec_http_headers = None

    rec_payload = b''.join(rec_payload)

    record = ArcWarcRecord(
        rec_headers.statusline,
        rec_headers.get_header('WARC-Type'),
        rec_headers,
        BytesIO(rec_payload),
        rec_http_headers,
        rec_headers.get_header('Content-Type'),
        None    # Recalculate Content-Length
    )

    warc_writer.write_record(record)
    logger.debug('Record {} written.'.format(rec_headers.get_header('WARC-Record-ID')))


if __name__ == '__main__':
    main()
