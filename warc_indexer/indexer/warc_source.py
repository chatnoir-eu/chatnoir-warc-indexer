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
            stream = self._open_file(file_meta.path, tracker.current_restriction().start)

            def stream_factory(pos):
                nonlocal stream
                stream = self._open_file(file_meta.path, stream.initial_offset + pos)
                return stream

            logger.info('Starting WARC file %s', file_meta.path)
            for record in warc_retry(ArchiveIterator(stream, **self._warc_args), stream_factory, seek=False):
                logger.debug('Reading WARC record %s', record.record_id)
                if not tracker.try_claim(record.stream_pos + stream.initial_offset):
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

    def _open_file(self, file_name, start_offset):
        """Get input file stream."""
        if file_name.startswith('s3://'):
            stream = self._open_s3_stream(file_name, start_offset)
            stream.initial_offset = start_offset
        else:
            stream = FileSystems.open(file_name, 'application/octet-stream',
                                      compression_type=CompressionTypes.UNCOMPRESSED)
            stream.initial_offset = 0
            if start_offset != 0:
                stream.seek(start_offset)

        return stream

    # noinspection PyProtectedMember
    def _open_s3_stream(self, file_name, start_offset=0, buffer_size=65536):
        """Open S3 stream more efficiently than the standard Beam implementation."""

        options = FileSystems._pipeline_options or RuntimeValueProvider.runtime_options
        s3_client = EfficientBoto3Client(options=options, range_offset=start_offset)
        s3io = S3IO(client=s3_client, options=options)

        downloader = S3Downloader(s3io.client, file_name, buffer_size=buffer_size)
        return io.BufferedReader(DownloaderStream(downloader, mode='rb'), buffer_size=buffer_size)


class EfficientBoto3Client(boto3_client.Client):
    # noinspection PyMissingConstructor
    def __init__(self, options, range_offset=0, retries=3, connect_timeout=60, read_timeout=240):
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

        self._request = None
        self._stream = None
        self._range_offset = range_offset
        self._pos = 0
        self._retries = retries

    def get_object_metadata(self, request):
        m = super().get_object_metadata(request)
        m.size = max(m.size - self._range_offset, 0)
        return m

    def get_stream(self, request, start):
        """Opens a stream object starting at the given position."""

        if self._request and (start != self._pos
                              or request.bucket != self._request.bucket
                              or request.object != self._request.object):
            self._stream.close()
            self._stream = None

        # noinspection PyProtectedMember
        if not self._stream or self._stream._raw_stream.closed:
            try:
                # noinspection PyProtectedMember
                self._stream = self.client.get_object(
                    Bucket=request.bucket,
                    Key=request.object,
                    Range=f'bytes={start + self._range_offset}-')['Body']
                self._request = request
                self._pos = start
            except Exception as e:
                message = e.response['Error'].get('Message', e.response['Error'].get('Code', ''))
                code = e.response['ResponseMetadata']['HTTPStatusCode']
                raise messages.S3ClientError(message, code)

        return self._stream

    def get_range(self, request, start, end):
        for i in range(self._retries):
            try:
                stream = self.get_stream(request, start)
                data = stream.read(end - start)
                self._pos += len(data)
                return data
            except boto_exception.BotoCoreError as e:
                # Read errors are more likely with long-lived connections, so retry if a read fails
                self._stream = None
                self._request = None
                logger.error('Boto3 read error (attempt %s/%s)', i + 1, self._retries)
                logger.exception(e)
                if i + 1 == self._retries:
                    raise messages.S3ClientError(str(e))
