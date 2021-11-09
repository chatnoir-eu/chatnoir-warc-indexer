import io
import sys

from apache_beam.io.aws.s3io import parse_s3_path, S3IO, S3Downloader
from apache_beam.io.aws.clients.s3.boto3_client import Client as S3Client
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import RangeTracker
from apache_beam.options.value_provider import RuntimeValueProvider
from fastwarc import warc


# noinspection PyAbstractClass
class WarcSource(FileBasedSource):
    """
    WARC file input source.
    """

    def __init__(self, file_pattern, validate=True, warc_args=None, freeze=False):
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
        def split_points_cb(stop_pos):
            if stop_pos >= range_tracker.last_attempted_record_start:
                return 0
            return RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_cb)

        from tqdm import tqdm
        try:
            with self._open_file(file_name, range_tracker.start_position(), range_tracker.stop_position()) as stream:
                for record in tqdm(warc.ArchiveIterator(stream, **self._warc_args)):
                    if not range_tracker.try_claim(record.stream_pos + stream.initial_offset):
                        break
                    if self.freeze:
                        record.freeze()
                    yield record
        except ValueError:
            # Stream closed
            return

    def _open_file(self, file_name, start_offset, end_offset):
        """Get input file stream."""
        if file_name.startswith('s3://'):
            stream = self._open_s3_stream(file_name, start_offset, end_offset)
            initial_offset = start_offset
        else:
            stream = self.open_file(file_name)
            initial_offset = 0
            if start_offset != 0:
                stream.seek(start_offset)

        stream.initial_offset = initial_offset
        return stream

    # noinspection PyProtectedMember
    @staticmethod
    def _open_s3_stream(file_name, start, end=None, buffer_size=65536):
        """Open S3 stream more efficiently (standard Beam pipeline is about 10x slower)."""
        s3io = S3IO(options=FileSystems._pipeline_options or RuntimeValueProvider.runtime_options)

        bucket, name = parse_s3_path(file_name)

        boto_response = s3io.client.client.get_object(
            Bucket=bucket,
            Key=name,
            Range='bytes={}-{}'.format(start, end - 1 if end else ''))['Body']._raw_stream

        return io.BufferedReader(boto_response, buffer_size=buffer_size)
