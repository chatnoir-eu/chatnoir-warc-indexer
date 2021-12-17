# Copyright 2021 Janek Bevendorff
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import logging
import time

import apache_beam as beam
from apache_beam.io.aws.s3io import S3IO, S3Downloader
from apache_beam.io.aws.clients.s3 import boto3_client, messages
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.utils import retry
from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
import apache_beam.transforms.window as window
from fastwarc.warc import ArchiveIterator
from resiliparse.itertools import warc_retry

try:
    import boto3
    import botocore.client as boto_client
    import botocore.exceptions as boto_exception
except ModuleNotFoundError:
    boto3 = None
    boto_client = None
    boto_exception = None


logger = logging.getLogger()


class WarcInput(beam.PTransform):
    def __init__(self, file_pattern, warc_args=None, freeze=True):
        """
        :param file_pattern: input file glob pattern
        :param warc_args: arguments to pass to :class:`fastwarc.warc.ArchiveIterator`
        :param freeze: freeze returned records
        """
        super().__init__()
        self._file_matcher = MatchFiles(file_pattern)
        self._warc_reader = _WarcReader(warc_args, freeze)

    def expand(self, pcoll):
        return pcoll | self._file_matcher | beam.Reshuffle() | beam.ParDo(self._warc_reader)


class _WarcRestrictionProvider(beam.transforms.core.RestrictionProvider):
    def initial_restriction(self, file_meta):
        return OffsetRange(0, file_meta.size_in_bytes)

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, file_meta, restriction):
        return min(file_meta.size_in_bytes, restriction.stop - restriction.start)


# noinspection PyAbstractClass
class _WarcReader(beam.DoFn):
    """
    WARC file input source.
    """

    def __init__(self, warc_args, freeze):
        super().__init__()
        self._warc_args = warc_args
        self._freeze = freeze

    # noinspection PyMethodOverriding
    def process(self, file_meta, tracker=beam.DoFn.RestrictionParam(_WarcRestrictionProvider())):
        """
        Read and return WARC records.

        :param file_meta: input file metadata
        :param tracker: input range tracker
        :return: tuple of (file name, WARC record)
        """

        stream = None
        record = None
        try:
            def stream_factory(pos):
                nonlocal stream
                stream = self._open_file(file_meta.path)
                if pos != 0:
                    stream.seek(pos)
                return stream

            stream = stream_factory(tracker.current_restriction().start)
            logger.info('Starting WARC file %s', file_meta.path)
            for record in warc_retry(ArchiveIterator(stream, **self._warc_args), stream_factory, seek=False):
                logger.debug('Reading WARC record %s', record.record_id)
                if not tracker.try_claim(record.stream_pos):
                    break
                if self._freeze:
                    record.freeze()

                yield window.TimestampedValue((file_meta.path, record), int(time.time()))
            else:
                tracker.try_claim(tracker.current_restriction().stop)
            logger.info('Completed WARC file %s', file_meta.path)
        except Exception as e:
            if record:
                logger.error('WARC reader failed in %s past record %s (pos: %s).',
                             file_meta.path, record.record_id, record.stream_pos)
            else:
                logger.error('WARC reader failed in %s', file_meta.path)
            logger.exception(e)
        finally:
            if stream and not stream.closed:
                stream.close()

    def _open_file(self, file_name):
        """Get input file stream."""
        if file_name.startswith('s3://'):
            stream = self._open_s3_stream(file_name)
        else:
            stream = FileSystems.open(file_name, 'application/octet-stream',
                                      compression_type=CompressionTypes.UNCOMPRESSED)

        return stream

    # noinspection PyProtectedMember
    def _open_s3_stream(self, file_name, buffer_size=65536):
        """Open S3 stream more efficiently than the standard Beam implementation."""

        options = FileSystems._pipeline_options or RuntimeValueProvider.runtime_options
        s3_client = EfficientBoto3Client(options=options)
        s3io = S3IO(client=s3_client, options=options)

        downloader = S3Downloader(s3io.client, file_name, buffer_size=buffer_size)
        return io.BufferedReader(DownloaderStream(downloader, mode='rb'), buffer_size=buffer_size)


def get_http_error_code(exc):
    if hasattr(exc, 'response'):
        return exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    return None


class EfficientBoto3Client(boto3_client.Client):
    # noinspection PyMissingConstructor
    def __init__(self, options, connect_timeout=60, read_timeout=240):
        if boto3 is None:
            raise ModuleNotFoundError('Missing boto3 requirement')

        if isinstance(options, pipeline_options.PipelineOptions):
            options = options.get_all_options()

        session = boto3.session.Session()
        self.client = session.client(
            service_name='s3',
            region_name=options.get('s3_region_name'),
            api_version=options.get('s3_api_version'),
            use_ssl=not options.get('s3_disable_ssl', False),
            verify=options.get('s3_verify'),
            endpoint_url=options.get('s3_endpoint_url'),
            aws_access_key_id=options.get('s3_access_key_id'),
            aws_secret_access_key=options.get('s3_secret_access_key'),
            aws_session_token=options.get('s3_session_token'),
            config=boto_client.Config(
                connect_timeout=connect_timeout,
                read_timeout=read_timeout
            )
        )

        self._download_request = None
        self._download_stream = None
        self._download_pos = 0

    # noinspection PyProtectedMember
    def get_stream(self, request, start):
        """Opens a stream object starting at the given position.

        Args:
          request: (GetRequest) request
          start: (int) start offset
        Returns:
          (Stream) Boto3 stream object.
        """

        if self._download_request and (
                start != self._download_pos
                or request.bucket != self._download_request.bucket
                or request.object != self._download_request.object):
            self._download_stream.close()
            self._download_stream = None

        # noinspection PyProtectedMember
        if not self._download_stream or self._download_stream._raw_stream.closed:
            try:
                self._download_stream = self.client.get_object(
                    Bucket=request.bucket,
                    Key=request.object,
                    Range='bytes={}-'.format(start))['Body']
                self._download_request = request
                self._download_pos = start
            except Exception as e:
                raise messages.S3ClientError(str(e), get_http_error_code(e))

        return self._download_stream

    @retry.with_exponential_backoff()
    def get_range(self, request, start, end):
        r"""Retrieves an object's contents.

          Args:
            request: (GetRequest) request
            start: (int) start offset
            end: (int) end offset (exclusive)
          Returns:
            (bytes) The response message.
          """
        for i in range(2):
            try:
                stream = self.get_stream(request, start)
                data = stream.read(end - start)
                self._download_pos += len(data)
                return data
            except Exception as e:
                self._download_stream = None
                self._download_request = None
                if i == 0:
                    # Read errors are likely with long-lived connections, retry immediately if a read fails once
                    continue
                if isinstance(e, messages.S3ClientError):
                    e.code = 500
                    raise e
                raise messages.S3ClientError(str(e), get_http_error_code(e) or 500)
