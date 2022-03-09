import apache_beam as beam
import apache_beam.typehints as t
from apache_beam.coders import coders
from apache_beam.io.filesystems import CompressionTypes, FileSystems
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker


DEFAULT_DESIRED_SPLIT_SIZE = 64 * 1024 * 1024  # 64 MiB


class UnfusedReadFromText(beam.PTransform):
    def __init__(self,
                 file_pattern,
                 shuffle_names=False,
                 desired_split_size=DEFAULT_DESIRED_SPLIT_SIZE,
                 min_split_size=1024,
                 compression_type=CompressionTypes.AUTO,
                 coder=coders.StrUtf8Coder()):
        """
        Read text from files in parallel.

        Can be used to parallelize the processing of large individual text files with newline-delimited
        records. Unlike :class:`apache_beam.io.textio.ReadFromText`, :class:`UnfusedReadFromText` prevents
        fusion of the file splits by opportunistically generating splits and shuffling them before
        actually reading the file contents. This prevents input bottlenecks.

        :param file_pattern: input file glob
        :param shuffle_names: shuffle matched file names before deriving splits (adds one more fusion
                              break, should be enabled if ``file_pattern`` matches many files or if
                              input files are compressed and cannot be split)
        :param desired_split_size: desired file split size in bytes
        :param min_split_size: minimum file split size in bytes
        :param compression_type: file compression type, will determine whether individual files are splittable
        :param coder: coder for decoding file contents
        """
        super().__init__()
        self._file_matcher = MatchFiles(file_pattern)
        self.gen_splits = _GenSplits(desired_split_size, min_split_size, compression_type)
        self.read_splits = _ReadSplits(compression_type, coder)
        self.shuffle_names = shuffle_names

    def expand(self, pcoll):
        pcoll |= self._file_matcher
        if self.shuffle_names:
            pcoll |= 'Shuffle Filenames' >> beam.Reshuffle()
        return (pcoll
                | beam.ParDo(self.gen_splits)
                | 'Shuffle Splits' >> beam.Reshuffle()
                | beam.ParDo(self.read_splits))


class _GenSplits(beam.DoFn):
    def __init__(self, desired_split_size, min_split_size, compression_type):
        """Create initial splits based on the text file size."""
        super().__init__()
        self.desired_split_size = max(min_split_size, desired_split_size)
        self.min_split_size = min_split_size
        self.compression_type = compression_type

    def process(self, element: FileMetadata):
        comp_type = self.compression_type
        if self.compression_type == CompressionTypes.AUTO:
            comp_type = CompressionTypes.detect_compression_type(element.path)

        if comp_type != CompressionTypes.UNCOMPRESSED:
            yield element, (0, element.size_in_bytes)
            return

        for start_offset in range(0, element.size_in_bytes, self.desired_split_size):
            end_offset = min(start_offset + self.desired_split_size, element.size_in_bytes)
            if element.size_in_bytes - end_offset < self.min_split_size:
                yield element, (start_offset, element.size_in_bytes)
                break
            yield element, (start_offset, end_offset)


class _FileSplitRestrictionProvider(beam.transforms.core.RestrictionProvider):
    def initial_restriction(self, file_split):
        return OffsetRange(file_split[1][0], file_split[1][1])

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, file_split, restriction):
        return file_split[1][1] - file_split[1][0]


class _ReadSplits(beam.DoFn):
    def __init__(self, compression_type, coder):
        """Read text lines from splits."""
        super().__init__()
        self.compression_type = compression_type
        self.coder = coder

    def process(self,
                element: t.KV[FileMetadata, t.Tuple[int, int]],
                tracker=beam.DoFn.RestrictionParam(_FileSplitRestrictionProvider())):
        file_meta, _ = element

        restriction = tracker.current_restriction()
        pos = restriction.start

        with FileSystems.open(file_meta.path,
                              mime_type=CompressionTypes.mime_type(self.compression_type),
                              compression_type=self.compression_type) as file:
            if pos > 0:
                # Seek to one character before the starting position and discard the first line.
                # This avoids skipping lines if files are split exactly at newlines.
                file.seek(pos - 1)
                file.readline()

            while tracker.try_claim(file.tell()):
                line = file.readline()[:-1]     # Strip trailing newline character
                if not line:
                    raise IOError(f'Unexpected EOF at {file.tell()}')
                if self.coder:
                    line = self.coder.decode(line)
                yield line
