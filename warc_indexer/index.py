#!/usr/bin/env python3
import logging
import os
import sys
from typing import Dict
from urllib.parse import urlparse
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import click
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
import html2text
from fastwarc.warc import WarcRecord, WarcRecordType
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast as lang_detect_fast

import es_utils
import lib
from warc_source import WarcSource
from time import monotonic


logger = logging.getLogger()
PATH = os.path.dirname(__file__)


@click.group()
def main():
    pass


# def embed_fasttext(text):
#     global ft
#     if ft is None:
#         # Load fasttext model
#         ft = fasttext.load_model('models/crawl-300d-2M-subword.bin')
#         print('Dimension of fasttext', ft.get_dimension())
#     else:
#         vectors = ft.get_word_vector(str(text))
#         return vectors.tolist()


def uuid_prefix_partitioner(key, num_partitions):
    return uuid.UUID(key).int * num_partitions // pow(16, 32)


@main.command()
@click.argument('meta-index')
@click.argument('data-index')
@click.option('--shards-meta', help='Number of shards for meta index', type=int, default=10, show_default=True)
@click.option('--shards-data', help='Number of shards for data index', type=int, default=40, show_default=True)
@click.option('--replicas', help='Number of replicas for indexing (should be 0 or 1)',
              type=int, default=1, show_default=True)
def index_setup(meta_index, data_index, shards_meta, shards_data, replicas):
    """
    Set up meta data and data indices if they don't already exist.
    """
    click.echo('Setting up indices if the do not exist.')

    import conf.data_index
    import conf.meta_index

    conf.meta_index.SETTINGS.update(dict(number_of_shards=shards_meta, number_of_replicas=replicas))
    conf.data_index.SETTINGS.update(dict(number_of_shards=shards_data, number_of_replicas=replicas))

    try:
        es_client = es_utils.get_client(**lib.get_config()['elasticsearch'])
        es_utils.ensure_index(es_client, data_index, conf.data_index.SETTINGS, conf.data_index.MAPPING)
        es_utils.ensure_index(es_client, meta_index, conf.meta_index.SETTINGS, conf.meta_index.MAPPING)
    except TransportError as e:
        click.echo(f'ERROR: {e.error}', err=True)
        if len(e.args) > 2:
            click.echo(e.args[2]["error"]["root_cause"][0]["reason"], err=True)

# CRASH
# 152285988 <urn:uuid:1781ffb1-b8c4-4c6b-92a9-7f423f3c0241>
# 152297725 <urn:uuid:b290f5a8-fc7a-4e28-afc8-14d3c1451f5b>
# 152311349 <urn:uuid:8143f718-47d1-42e4-b6ed-c44156ad98ea>
# 152321628 <urn:uuid:b8d83f46-51aa-4565-9ef5-04117d09476a>

@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('input-glob')
@click.argument('meta-index')
@click.argument('data-index')
@click.argument('id-prefix')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
def index(input_glob, meta_index, data_index, id_prefix, beam_args):
    """
    Index WARC contents.

    WARC records from``INPUT_GLOB`` will be index to index ``DATA_INDEX`` with WARC metadata
    and offsets indexed to ``META_INDEX``.

    ``ID_PREFIX`` is used for calculating document UUIDs.
    """

    sys.argv[1:] = beam_args
    options = PipelineOptions(**lib.get_config()['pipeline_opts'])

    click.echo(f'Starting pipeline to index "{input_glob}"...')
    start = monotonic()
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Iterate WARCs' >> beam.io.Read(
                    WarcSource(input_glob, warc_args=dict(record_types=int(WarcRecordType.response))))
            | 'Process Records' >> beam.ParDo(ProcessRecord(id_prefix))
            # | beam.Map(lambda x: print(x[1]['source_offset'], x[1]['warc_record_id']))
        )

    click.echo(f'Time taken: {monotonic() - start:.2f}s')


class SkipRecord(Exception):
    pass


# noinspection PyAbstractClass
class ProcessRecord(beam.DoFn):
    def __init__(self, doc_id_prefix: str):
        super().__init__()
        self.doc_id_prefix = doc_id_prefix

    # noinspection PyMethodOverriding
    def process(self, element):
        """
        Process WARC record.

        :param element: tuple of (file name, WARCRecord)
        :return: tuple of (UUID, Metadata, Payload)
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
        webis_uuid = lib.webis_uuid(self.doc_id_prefix, doc_id)
        content_bytes = warc_record.reader.read()

        try:
            meta = self.create_metadata(file_name, warc_record, content_bytes)
            payload = self.create_payload(meta, content_bytes)
            yield webis_uuid, meta, payload
        except SkipRecord as reason:
            logger.info(f'Skipping document {doc_id}, reason: {reason}')

    def create_metadata(self, file_name: str, warc_record: WarcRecord, content_bytes: bytes):
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
            meta['warc_date'] = lib.clip_warc_date(meta['warc_date'])

        return meta

    def create_payload(self, metadata: Dict[str, str], content_bytes: bytes):
        """
        Parse WARC record payload into an index document.

        :param metadata: WARC metadata dict as created by :meth:`create_metadata`
        :param content_bytes: raw payload as bytes
        :return: index document dict
        """

        content_str = bytes_to_str(content_bytes, metadata['content_encoding'])
        lang = lang_detect_fast(content_str)
        parse_url = urlparse(metadata['warc_target_uri'])
        html_tree = HTMLTree.parse(content_str)

        if not html_tree.body:
            raise SkipRecord('No body')

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

        plain_text = ''
        # try:
        #     text_maker = html2text.HTML2Text()
        #     text_maker.ignore_links = True
        #     plain_text = text_maker.handle(str(html_tree.body))
        # except:
        #     pass

        if len(plain_text) < 200:
            raise SkipRecord(f'Document too short ({len(plain_text)} codepoints)')

        index_doc.update({
            f'title_lang_{lang}': lib.get_document_title(html_tree),
            'meta_keywords': lib.get_document_meta_keywords(html_tree),
            f'meta_desc_lang_{lang}': lib.get_document_meta_desc(html_tree),
            f'body_lang_{lang}': plain_text,
            f'full_body_lang_{lang}': lib.get_full_body_text_content(html_tree),
            f'headings_lang_{lang}': lib.get_document_headings(html_tree, 3)
        })

        # Prune keys with empty or None values
        doc_keys = list(index_doc.keys())
        for k in doc_keys:
            if not index_doc[k]:
                del index_doc[k]

        return index_doc


if __name__ == '__main__':
    main()
