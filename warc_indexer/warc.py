from apache_beam.io.filebasedsource import FileBasedSource
from fastwarc import warc


class WarcSource(FileBasedSource):
    def read_records(self, file_name, offset_range_tracker):
        stream = self.open_file(file_name)

        if offset_range_tracker.start_position() != 0:
            stream.seek(offset_range_tracker.start_position())

        for record in warc.ArchiveIterator(stream):
            if not offset_range_tracker.try_claim(record.stream_pos):
                break
            yield record
