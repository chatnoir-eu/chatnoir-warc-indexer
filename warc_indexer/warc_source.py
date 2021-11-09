import io
import sys

from apache_beam.io.aws.s3io import S3IO, S3Downloader
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

        with self._open_file(file_name, range_tracker.start_position()) as stream:
            for record in warc.ArchiveIterator(stream, **self._warc_args):
                if not range_tracker.try_claim(record.stream_pos + stream.initial_offset):
                    break
                if self.freeze:
                    record.freeze()
                yield record

    def _open_file(self, file_name, offset):
        """Get input file stream."""
        if file_name.startswith('s3://'):
            stream = self._open_s3_offset(file_name, offset)
            initial_offset = offset
        else:
            stream = self.open_file(file_name)
            initial_offset = 0
            if offset != 0:
                stream.seek(offset)

        stream.initial_offset = initial_offset
        return stream

    # noinspection PyProtectedMember
    @staticmethod
    def _open_s3_offset(file_name, offset, buffer_size=65536):
        """Open S3 stream more efficiently, particularly at non-zero offsets."""
        s3io = S3IO(options=FileSystems._pipeline_options or RuntimeValueProvider.runtime_options)
        downloader = DownloaderStream(S3Downloader(s3io.client, file_name, buffer_size), mode='r')
        downloader._position = offset
        return io.BufferedReader(downloader, buffer_size=buffer_size)
