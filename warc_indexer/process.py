from base64 import b64encode
from calendar import monthrange
import logging
import re
from urllib.parse import urlparse
import uuid

import apache_beam as beam
import apache_beam.typehints.typehints as t

from fastwarc import warc
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast as lang_detect_fast

from warc_indexer.es_sink import index_action

logger = logging.getLogger()


class SkipRecord(Exception):
    pass


MULTI_SPACE_REGEX = re.compile(r'\s{2,}')


# noinspection PyAbstractClass
class ProcessRecord(beam.DoFn):
    def __init__(self, doc_id_prefix: str, meta_index: str, data_index: str):
        super().__init__()
        self.doc_id_prefix = doc_id_prefix
        self.meta_index = meta_index
        self.data_index = data_index

    # noinspection PyMethodOverriding
    def process(self, element: t.Tuple[str, warc.WarcRecord]) -> \
            t.Iterable[t.KV[str, t.Tuple[t.Dict[str, str], t.Dict[str, str]]]]:
        """
        Process WARC record and turn it into Elasticsearch index actions.

        :param element: tuple of file name, WARCRecord
        :return: key-value pair of UUID, (Metadata, Payload)
        """
        file_name, warc_record = element
        doc_id = warc_record.headers.get('WARC-Record-ID')

        if not warc_record.headers.get('Content-Type', '').startswith('application/http'):
            logger.info(f'Skipping document {doc_id}, reason: Not an HTTP response')
            return

        if warc_record.content_length > 1024 * 1024:
            logger.info(f'Skipping document {doc_id}, reason: Document too short ({warc_record.content_length} bytes)')
            return

        if warc_record.content_length < 500:
            logger.info(f'Skipping document {doc_id}, reason: Document too short ({warc_record.content_length} bytes)')
            return

        doc_id = warc_record.headers.get('WARC-TREC-ID', warc_record.headers.get('WARC-Record-ID'))
        wuid = webis_uuid(self.doc_id_prefix, doc_id)
        content_bytes = warc_record.reader.read()

        try:
            meta = self.create_metadata(file_name, warc_record, content_bytes)
            payload = self.create_payload(meta, content_bytes)
            yield wuid, (
                index_action(wuid, self.meta_index, meta),
                index_action(wuid, self.data_index, payload)
            )
        except SkipRecord as reason:
            logger.info(f'Skipping document {doc_id}, reason: {reason}')

    @staticmethod
    def create_metadata(file_name: str, warc_record: warc.WarcRecord, content_bytes: bytes):
        """
        Parse WARC record into header dict and decoded content.

        :param file_name: WARC file name
        :param warc_record: WarcRecord instance (unconsumed, but with parsed HTTP)
        :param content_bytes: WarcRecord payload data as bytes
        :return: tuple of doc_uuid, (meta data, payload)
        """

        http_content_length = warc_record.content_length
        encoding = warc_record.http_charset or detect_encoding(content_bytes)

        meta = {
            'source_file': file_name,
            'source_offset': warc_record.stream_pos,
            **{h.replace('-', '_').lower(): v for h, v in warc_record.headers if h.startswith('WARC-')},
            'content_type': warc_record.headers.get('Content-Type'),
            'content_length': warc_record.headers.get('Content-Length'),
            'http_content_length': http_content_length,
            'http_content_type': warc_record.http_content_type,
            'content_encoding': encoding
        }

        if 'warc_date' in meta:
            # Fix buggy ClueWeb WARC-Date headers
            meta['warc_date'] = clip_warc_date(meta['warc_date'])

        return meta

    @staticmethod
    def create_payload(metadata: t.Dict[str, str], content_bytes: bytes):
        """
        Parse WARC record payload into an index document.

        :param metadata: WARC metadata dict as created by :meth:`create_metadata`
        :param content_bytes: raw payload as bytes
        :return: index document dict
        """

        content_str = bytes_to_str(content_bytes, metadata['content_encoding'])
        parse_url = urlparse(metadata['warc_target_uri'])
        html_tree = HTMLTree.parse(content_str)

        if not html_tree.body:
            raise SkipRecord('No body')

        content_full = extract_plain_text(html_tree.document, preserve_formatting=False)

        lang = lang_detect_fast(content_full)
        if lang[0] == 'en' and lang[1] > 1000:
            raise SkipRecord('Document does not look like a text document.')

        main_content = extract_plain_text(html_tree.body, main_content=True, preserve_formatting=False)
        if len(main_content) < 200:
            raise SkipRecord(f'Document too short ({len(main_content)} codepoints).')

        replacement_count = main_content.count('\ufffd')
        if replacement_count / len(main_content) > 0.1:
            raise SkipRecord(f'Document contains more than 10% Unicode replacement characters.')
        if replacement_count > 0:
            main_content = MULTI_SPACE_REGEX.sub(' ', main_content.replace('\ufffd', '')).strip()

        index_doc = {
            'warc_record_id': metadata.get('warc_record_id'),
            'warc_trec_id': metadata.get('warc_trec_id'),
            'date': metadata.get('warc_date'),
            'lang': lang,
            'body_length': len(content_str),
            'warc_target_uri': metadata.get('warc_target_uri'),
            'warc_target_hostname': parse_url.hostname,
            'warc_target_path': parse_url.path,
            'warc_target_query_string': parse_url.query,
            'content_type': metadata.get('http_content_type'),
        }

        index_doc.update({
            f'title_lang_{lang}': get_document_title(html_tree),
            'meta_keywords': get_document_meta_keywords(html_tree),
            f'meta_desc_lang_{lang}': get_document_meta_desc(html_tree),
            f'body_lang_{lang}': main_content,
            f'full_body_lang_{lang}': content_full,
            f'headings_lang_{lang}': get_document_headings(html_tree, 3)
        })

        # Prune keys with empty or None values
        doc_keys = list(index_doc.keys())
        for k in doc_keys:
            if not index_doc[k]:
                del index_doc[k]

        return index_doc


def webis_uuid(corpus_prefix: str, internal_id: str) -> str:
    """
    Calculate a Webis document UUID based on a corpus prefix and
    an internal (not necessarily universally unique) doc ID.

    :param corpus_prefix: corpus prefix (e.g., clueweb09, cc15, ...)
    :param internal_id: internal doc ID (e.g., clueweb09-en0044-22-32198)
    :return: Webis UUID as str
    """
    return b64encode(uuid.uuid5(uuid.NAMESPACE_URL, ':'.join((corpus_prefix, internal_id))).bytes).decode()


def clip_warc_date(date_val: str) -> str:
    """
    ClueWeb WARCs have buggy WARC-Date headers with values such as '2009-03-82T07:34:44-0700', causing indexing errors.
    This function clips the day part to the number of days a month has.

    :param date_val: potentially malformed ISO 8601 date value
    :return: fixed date if day is out of range else input unchanged
    """
    def c(y, m, d):
        return '{:02}'.format(min(int(d), monthrange(int(y), int(m))[1]))

    return re.sub(r'(\d{4})-(\d{2})-(\d+)',
                  lambda g: '{}-{}-{}'.format(g.group(1), g.group(2), c(g.group(1), g.group(2), g.group(3))), date_val)


def get_document_title(html_tree: HTMLTree) -> str:
    """
    Intelligently try to extract a document title.

    :param html_tree: Resiliparse HTML tree
    :return: title
    """
    title = html_tree.title.strip()
    if title:
        return title

    h1 = html_tree.body.query_selector('h1')
    if h1 and h1.text:
        return h1.text

    h2 = html_tree.body.query_selector('h2')
    if h2 and h2.text:
        return h2.text

    # title_cls = html_tree.body.query_selector('.title')
    # if title_cls:
    #     return title_cls.text

    return ''


def get_document_meta_desc(html_tree: HTMLTree) -> str:
    """
    Get document meta description

    :param html_tree: Resiliparse HTML tree
    :return: meta description
    """
    if not html_tree.head:
        return ''

    desc = html_tree.head.query_selector('meta[name="description"]')
    if not desc:
        return ''

    return desc.getattr('content', '').strip()


def get_document_meta_keywords(html_tree: HTMLTree) -> str:
    """
    Get document meta keywords as list

    :param html_tree: Resiliparse HTML tree
    :return: meta keywords
    """
    if not html_tree.head:
        return []

    keywords = html_tree.head.query_selector('meta[name="keywords"]')
    if not keywords:
        return []

    return [k.strip() for k in keywords.getattr('content', '').split(',')]


def get_document_headings(html_tree: HTMLTree, max_level: int = 3) -> t.List[str]:
    """
    Get a list of document headings up to a certain level

    :param html_tree: Resiliparse HTML tree
    :param max_level: maximum heading level to extract
    :return: list of headings
    """
    if not html_tree.head:
        return []

    headings = html_tree.head.query_selector_all(', '.join(f'h{i}' for i in range(1, max_level + 1)))
    return [k.text.strip() for k in headings]
