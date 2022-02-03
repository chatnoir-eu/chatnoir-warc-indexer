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

from base64 import b64encode
from calendar import monthrange
from dateutil.parser import parse as date_parse
from hashlib import blake2b
import logging
import re
from urllib.parse import urlparse
import uuid

import apache_beam as beam
import apache_beam.typehints.typehints as t

from fastwarc import warc
from apache_beam.metrics import Metrics
from resiliparse.parse.encoding import bytes_to_str, detect_encoding, detect_mime
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast as lang_detect_fast

from warc_indexer.indexer.es_sink import index_action

logger = logging.getLogger()


class SkipRecord(Exception):
    pass


MULTI_SPACE_REGEX = re.compile(r'\s{2,}')
MAX_DOCUMENT_SIZE = 1024 * 1024


class ProcessRecords(beam.PTransform):
    def __init__(self, doc_id_prefix: str, meta_index: str, data_index: str, always_index_meta: bool = False,
                 max_payload_size: int = MAX_DOCUMENT_SIZE, trust_http_content_type: bool = False):
        """
        Process a collection of WARC records and turn them into Elasticsearch index actions.

        :param doc_id_prefix: document UUID prefix
        :param meta_index: meta index name (required for index action creation)
        :param data_index: daata index name (required for index action creation)
        :param always_index_meta: always index a document's metadata, even if the payload document is skipped
        :param trust_http_content_type: unconditionally trust HTTP Content-Type, don't perform check for binary response
        """
        super().__init__()
        self.do_fn = ProcessRecord(doc_id_prefix, meta_index, data_index, always_index_meta,
                                   max_payload_size, trust_http_content_type)

    def expand(self, pcoll):
        return pcoll | beam.ParDo(self.do_fn)


# noinspection PyAbstractClass
class ProcessRecord(beam.DoFn):
    def __init__(self, doc_id_prefix, meta_index: str, data_index: str, always_index_meta: bool = False,
                 max_payload_size: int = MAX_DOCUMENT_SIZE, trust_http_content_type: bool = False):
        super().__init__()
        self.doc_id_prefix = doc_id_prefix
        self.meta_index = meta_index
        self.data_index = data_index
        self.always_index_meta = always_index_meta
        self.counter = Metrics.counter(self.__class__, 'warc_record_counter')
        self.max_payload_size = max_payload_size
        self.trust_http_content_type = trust_http_content_type

    # noinspection PyMethodOverriding
    def process(self, element: t.Tuple[str, warc.WarcRecord]) -> t.Iterable[t.KV[str, t.Dict[str, t.Any]]]:
        """
        Process a single WARC record and turn it into Elasticsearch index actions.

        :param element: tuple of file name, WARCRecord
        :return: iterable of (index name, index action) KV pairs
        """

        self.counter.inc(1)

        file_name, warc_record = element  # type: str, warc.WarcRecord
        doc_id = warc_record.headers.get('WARC-TREC-ID', warc_record.headers.get('WARC-Record-ID'))

        idx_id = None
        payload = None
        meta = None

        try:
            # Always skip non-HTTP responses, even if always_index_meta is True
            if not warc_record.headers.get('Content-Type', '').startswith('application/http'):
                if self.always_index_meta:
                    logger.warning(
                        'No meta document created for non-HTTP response despite "always create meta" setting.')
                raise SkipRecord('Not an HTTP response')

            webis_id = webis_uuid(self.doc_id_prefix, doc_id)
            record_time = int(date_parse(clip_warc_date(warc_record.headers.get('WARC-Date'))).timestamp() * 1000)
            idx_id = index_uuid(record_time, warc_record.stream_pos, file_name, webis_id)
            content_bytes = warc_record.reader.read(self.max_payload_size)

            # Always create meta object
            meta = self.create_metadata(webis_id, file_name, warc_record, content_bytes)

            if not warc_record.http_content_type or \
                    warc_record.http_content_type.lower() not in ['text/html', 'application/xhtml+xml', 'text/plain']:
                raise SkipRecord(f'Wrong Content-Type ({warc_record.http_content_type})')

            if warc_record.content_length > self.max_payload_size:
                raise SkipRecord(f'Document too big ({warc_record.content_length} bytes)')

            if warc_record.content_length < 200:
                raise SkipRecord(f'Document too short ({warc_record.content_length} bytes)')

            payload = self.create_payload(webis_id, meta, content_bytes)

        except SkipRecord as reason:
            logger.info('Skipping document %s, reason: %s', doc_id, reason)

        except Exception as e:
            logger.error('Skipping failed document %s. Error was:', doc_id)
            logger.exception(e)

        finally:
            if idx_id is None:
                return

            if meta is not None and (payload is not None or self.always_index_meta):
                yield self.meta_index, index_action(idx_id, self.meta_index, meta)
            if payload is not None:
                yield self.data_index, index_action(idx_id, self.data_index, payload)

    @staticmethod
    def create_metadata(doc_id, file_name: str, warc_record: warc.WarcRecord, content_bytes: bytes):
        """
        Parse WARC record into header dict and decoded content.

        :param doc_id: Webis document UUID
        :param file_name: WARC file name
        :param warc_record: WarcRecord instance (unconsumed, but with parsed HTTP)
        :param content_bytes: WarcRecord payload data as bytes
        :return: tuple of doc_uuid, (meta data, payload)
        """

        http_content_length = warc_record.content_length
        encoding = warc_record.http_charset or detect_encoding(content_bytes)
        http_date = None
        try:
            http_date = date_parse(warc_record.http_headers.get('Date')).isoformat()
        except Exception as e:
            logger.warning('Error parsing HTTP Date header: %s', str(e))

        meta = {
            'uuid': doc_id,
            'source_file': file_name,
            'source_offset': warc_record.stream_pos,
            **{h.replace('-', '_').lower(): v for h, v in warc_record.headers if h.startswith('WARC-')},
            'content_type': warc_record.headers.get('Content-Type'),
            'content_length': warc_record.headers.get('Content-Length'),
            'http_content_length': http_content_length,
            'http_content_type': warc_record.http_content_type,
            'http_date': http_date,
            'content_encoding': encoding
        }

        if 'warc_date' in meta:
            # Fix buggy ClueWeb WARC-Date headers
            meta['warc_date'] = clip_warc_date(meta['warc_date'])

        return meta

    def create_payload(self, doc_id, metadata: t.Dict[str, str], content_bytes: bytes):
        """
        Parse WARC record payload into an index document.

        :param doc_id: Webis document UUID
        :param metadata: WARC metadata dict as created by :meth:`create_metadata`
        :param content_bytes: raw payload as bytes
        :return: index document dict
        """

        if not self.trust_http_content_type:
            mime_type = detect_mime(content_bytes)
            if mime_type not in ['text/html', 'application/xhtml+xml', 'text/plain']:
                raise SkipRecord(f'Document does not look like a text document (looks like {mime_type}).')

        content_str = bytes_to_str(content_bytes, metadata['content_encoding'])

        parse_url = urlparse(metadata['warc_target_uri'])
        html_tree = HTMLTree.parse(content_str)

        if not html_tree.body:
            raise SkipRecord('No body')

        content_full = extract_plain_text(html_tree, alt_texts=True, preserve_formatting=False)
        if not content_full:
            raise SkipRecord('Document empty after full content extraction')

        replacement_count = content_full.count('\ufffd')
        if replacement_count / len(content_full) > 0.1:
            raise SkipRecord(f'Document contains more than 10% Unicode replacement characters.')
        if replacement_count > 0:
            content_full = MULTI_SPACE_REGEX.sub(' ', content_full.replace('\ufffd', '')).strip()

        lang, lang_score = lang_detect_fast(content_full)

        main_content = extract_plain_text(html_tree, main_content=True, alt_texts=True,
                                          preserve_formatting=True, list_bullets=False)
        if len(main_content) < 200:
            raise SkipRecord(f'Main content too short ({len(main_content)} codepoints).')

        index_doc = {
            'uuid': doc_id,
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
            f'meta_keywords_{lang}': get_document_meta_keywords(html_tree)[:8192],
            f'meta_desc_lang_{lang}': get_document_meta_desc(html_tree)[:8192],
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


def urlsafe_b64(input_str):
    """
    Make a Base64 input string URL-safe by replacing + with _ and / with -.

    :param input_str: raw Base64 string
    :return: URL-safe string translation
    """
    return input_str.translate({47: 45, 43: 95})


def webis_uuid(corpus_prefix: str, internal_id: str) -> str:
    """
    Calculate a URL-safe Webis document UUID based on a corpus prefix and
    an internal (not necessarily universally unique) doc ID.

    :param corpus_prefix: corpus prefix (e.g., clueweb09, cc15, ...)
    :param internal_id: internal doc ID (e.g., clueweb09-en0044-22-32198)
    :return: Webis UUID as truncated and URL-safe Base64 string
    """
    return urlsafe_b64(b64encode(uuid.uuid5(
        uuid.NAMESPACE_URL, ':'.join((corpus_prefix, internal_id))).bytes)[:-2].decode())


def index_uuid(unix_time_ms, warc_pos, warc_name, doc_id):
    """
    Calculate an index-friendly and URL-safe time-based UUIDv1 for a document.

    :param unix_time_ms: 64-bit UNIX timestamp of the document in milliseconds
    :param warc_pos: character offset in the WARC file
    :param warc_name: WARC file name string
    :param doc_id: document Webis UUID string
    :return: index UUID as truncated and URL-safe Base64 string
    """
    mask_low = (1 << 32) - 1
    mask_mid = ((1 << 16) - 1) << 32
    time_low = unix_time_ms & mask_low
    time_mid = (unix_time_ms & mask_mid) >> 32

    warc_pos = warc_pos & ((1 << 32) - 1)
    time_hi_version = ((warc_pos >> 16) & 0x3FFF) | 0x1000

    clock_seq = warc_pos & 0xFFFF
    clock_seq_hi_variant = ((clock_seq >> 8) & 0x3F) | 0x80
    clock_seq_low = clock_seq & 0x00FF

    name_hash = blake2b(warc_name.encode(), digest_size=3).digest()
    id_hash = blake2b(doc_id.encode(), digest_size=3).digest()
    node = int.from_bytes(name_hash + id_hash, 'big')

    u = uuid.UUID(fields=(time_low, time_mid, time_hi_version, clock_seq_hi_variant, clock_seq_low, node))
    return urlsafe_b64(b64encode(u.bytes)[:-2].decode())


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


WS_REGEX = re.compile(r'\s+')


def ws_collapse(text):
    """Collapse white space and trim input string."""
    return WS_REGEX.sub(' ', text).strip()


def get_document_title(html_tree: HTMLTree) -> str:
    """
    Intelligently try to extract a document title.

    :param html_tree: Resiliparse HTML tree
    :return: title
    """
    title = html_tree.title.strip()
    if title:
        return ws_collapse(title)

    h1 = html_tree.body.query_selector('h1')
    if h1 and h1.text:
        return ws_collapse(h1.text)

    h2 = html_tree.body.query_selector('h2')
    if h2 and h2.text:
        return ws_collapse(h2.text,)

    title_cls = html_tree.body.query_selector('.title')
    if title_cls:
        return ws_collapse(title_cls.text)

    return ''


def get_document_meta_desc(html_tree: HTMLTree) -> str:
    """
    Get document meta description.

    :param html_tree: Resiliparse HTML tree
    :return: meta description
    """
    if not html_tree.head:
        return ''

    desc = html_tree.head.query_selector('meta[name="description"]')
    if not desc:
        return ''

    return ws_collapse(desc.getattr('content', ''))


def get_document_meta_keywords(html_tree: HTMLTree, max_len: int = 80, limit: int = 30) -> t.List[str]:
    """
    Get list of deduplicated and lower-cased document meta keywords.

    :param html_tree: Resiliparse HTML tree
    :param max_len: cut off individual keywords after this many characters
    :param limit: limit list to this many keywords
    :return: meta keywords
    """
    if not html_tree.head:
        return []

    keywords = html_tree.head.query_selector('meta[name="keywords"]')
    if not keywords:
        return []

    return list(set(ws_collapse(k)[:max_len].lower() for k in keywords.getattr('content', '').split(',')))[:limit]


def get_document_headings(html_tree: HTMLTree, max_level: int = 3) -> t.List[str]:
    """
    Get a list of document headings up to a certain level.

    :param html_tree: Resiliparse HTML tree
    :param max_level: maximum heading level to extract
    :return: list of headings
    """
    if not html_tree.head:
        return []

    headings = html_tree.head.query_selector_all(', '.join(f'h{i}' for i in range(1, max_level + 1)))
    return [ws_collapse(k.text) for k in headings]
