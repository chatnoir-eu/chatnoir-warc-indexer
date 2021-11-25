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
import sys
import time

import apache_beam as beam
from apache_beam.io.aws.s3io import S3IO, S3Downloader
from apache_beam.io.aws.clients.s3 import boto3_client, messages
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import RangeTracker
from apache_beam.options.value_provider import RuntimeValueProvider
import apache_beam.transforms.window as window
from fastwarc import warc

logger = logging.getLogger()


class WarcSource(beam.PTransform):
    def __init__(self, file_pattern, validate=True, warc_args=None, freeze=True):
        """
        :param file_pattern: input file glob pattern
        :param warc_args: arguments to pass to :class:`fastwarc.warc.ArchiveIterator`
        :param freeze: freeze returned records
        """
        super().__init__()
        self.file_pattern = file_pattern
        self.warc_args = warc_args
        self.freeze = freeze
        self.validate = validate

    def expand(self, pcoll):
        return pcoll | beam.io.Read(_WarcSource(self.file_pattern, self.validate, self.warc_args, self.freeze))


# noinspection PyAbstractClass
class _WarcSource(FileBasedSource):
    """
    WARC file input source.
    """

    def __init__(self, file_pattern, validate=True, warc_args=None, freeze=True):
        """
        :param file_pattern: input file glob pattern
        :param validate: verify that file exists
        :param warc_args: arguments to pass to :class:`fastwarc.warc.ArchiveIterator`
        :param freeze: freeze returned records
        """
        super().__init__(file_pattern,
                         sys.maxsize,                    # WARCs are not arbitrarily splittable without an index
                         CompressionTypes.UNCOMPRESSED,  # Never decompress a file in Beam, FastWARC can do it faster
                         True,                           # Source is generally splittable, just not blindly up-front
                         validate)
        self._warc_args = warc_args or {}
        self.freeze = freeze

    def read_records(self, file_name, range_tracker):
        """
        Read and return WARC records.

        :param file_name: input file name
        :param range_tracker: input range tracker
        :return: tuple of (file name, WARC record)
        """

        def split_points_cb(stop_pos):
            if stop_pos >= range_tracker.last_attempted_record_start:
                return 0
            return RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_cb)

        count = 0
        with self._open_file(file_name, range_tracker.start_position()) as stream:
            for record in warc.ArchiveIterator(stream, **self._warc_args):
                count += 1
                if not range_tracker.try_claim(record.stream_pos + stream.initial_offset):
                    break
                if self.freeze:
                    record.freeze()

                yield window.TimestampedValue((file_name, record), int(time.time()))

    def _open_file(self, file_name, start_offset):
        """Get input file stream."""
        if file_name.startswith('s3://'):
            stream = self._open_s3_stream(file_name, start_offset)
            initial_offset = start_offset
        else:
            stream = self.open_file(file_name)
            initial_offset = 0
            if start_offset != 0:
                stream.seek(start_offset)

        stream.initial_offset = initial_offset
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
    def __init__(self, options, range_offset=0):
        super().__init__(options)
        self._request = None
        self._stream = None
        self._range_offset = range_offset
        self._pos = 0

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
                self._stream = self.client.get_object(
                    Bucket=request.bucket,
                    Key=request.object,
                    Range='bytes={}-'.format(start + self._range_offset))['Body']
                self._request = request
                self._pos = start
            except Exception as e:
                message = e.response['Error'].get('Message', e.response['Error'].get('Code', ''))
                code = e.response['ResponseMetadata']['HTTPStatusCode']
                raise messages.S3ClientError(message, code)

        return self._stream

    def get_range(self, request, start, end):
        stream = self.get_stream(request, start)
        data = stream.read(end - start)
        self._pos += len(data)
        return data
