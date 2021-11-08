#!/usr/bin/env python3

from functools import partial
import logging
import os
import re
from typing import Dict
from urllib.parse import urlparse
import uuid

import apache_beam as beam
import boto3
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import S3Options
import click
from elasticsearch import Elasticsearch
import html2text
from fastwarc.warc import ArchiveIterator, WarcRecord, WarcRecordType
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast as lang_detect_fast

import es_utils
import lib
import warc
from warc import WarcSource


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
@click.argument('s3-prefix')
@click.argument('meta-index')
@click.argument('content-index')
@click.argument('doc-id-prefix')
# @click.option('-b', '--batch-size', type=int, default=1000, show_default=True,
#               help='Number of input files to process in one batch (last batch may be larger by up to 50%).')
# @click.option('-f', '--path-filter', type=str, default='', help='Input path prefix filter')
# @click.option('-c', '--chunk-size', type=int, default=400, show_default=True, help='Chunk size of documents to index')
# @click.option('-p', '--index-parallelism', type=int, default=130, show_default=True,
#               help='Number of partitions for indexing (should be smaller or equal number of workers)')
def index(s3_prefix, meta_index, content_index, doc_id_prefix):
    """
    Index offsets of WARC documents into Elasticsearch.
    """

    logger.info('Creating indices if the do not exist.')
    # import conf.data_index
    # import conf.meta_index
    # es_utils.ensure_index(content_index, conf.data_index.SETTINGS, conf.data_index.MAPPING)
    # es_utils.ensure_index(meta_index, conf.meta_index.SETTINGS, conf.meta_index.MAPPING)

    # s3 = lib.get_s3_resource()
    logger.debug('Retrieving input file list...')
    # file_list = list(o.key for o in s3.Bucket(s3_bucket).objects.filter(Prefix=path_filter))


    options = S3Options(**lib.get_config()['s3'])
    # options.S3Options
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        indexed = (
            pipeline
            | WarcSource('s3://corpus-commoncrawl-main-2017-22/crawl-data/CC-MAIN-2017-22/segments/1495463615105.83/warc/CC-MAIN-20170530124450-20170530144450-00511.warc.gz')
        )

    #
    # s3 = lib.get_s3_resource()
    # logger.debug('Retrieving file list...')
    # file_list = list(o.key for o in s3.Bucket(s3_bucket).objects.filter(Prefix=path_filter))
    #
    # logger.info('Retrieved file list.')
    #
    # # Avoid last batch being smaller than 0.5 * batch_size and append remainder to previous batch instead
    # num_batches = int(len(file_list) / batch_size + 0.5)
    #
    # for i in range(num_batches):
    #     logger.info('Starting batch {} of {}...'.format(i, num_batches))
    #
    #     slice_start = i * batch_size
    #     slice_end = slice_start + batch_size if i + 1 < num_batches else len(file_list)
    #
    #     (sc.parallelize(file_list[slice_start:slice_end], numSlices=slice_end - slice_start)
    #      .flatMap(partial(parse_warc_stream, doc_id_prefix=doc_id_prefix, bucket=s3_bucket))
    #      .map(partial(parse_record, discard_content=False), preservesPartitioning=True)
    #      .flatMap(partial(create_index_actions, meta_index=meta_index, content_index=content_index),
    #               preservesPartitioning=True)
    #      .repartitionAndSortWithinPartitions(index_parallelism,
    #                                          partial(uuid_prefix_partitioner, num_partitions=index_parallelism))
    #      .cache()
    #      .foreachPartition(partial(index_partition, chunk_size=chunk_size)))
    #
    #     logger.info('Completed batch {}.'.format(i))


class SkipRecord(Exception):
    pass


# noinspection PyAbstractClass
class WarcIndexFn(beam.DoFn):
    def __init__(self, es_client: Elasticsearch, s3_bucket: str, doc_id_prefix: str):
        super().__init__()
        self.s3_bucket = s3_bucket
        self.doc_id_prefix = doc_id_prefix
        self.es_client = es_client

    # noinspection PyMethodOverriding
    def process(self, warc_name):
        if not warc_name.endswith('.warc.gz'):
            logger.warning('Skipping non-WARC file {}'.format(warc_name))
            return []

        warc = lib.get_s3_resource().Object(self.s3_bucket, warc_name).get()['Body']
        for record in ArchiveIterator(warc, record_types=WarcRecordType.response):
            doc_id = record.headers.get('WARC-Record-ID')

            if not record.headers.get('Content-Type', '').startswith('application/http'):
                logger.info(f'Skipping document {doc_id}, reason: Not an HTTP response')
                continue

            if record.content_length > 1024 * 1024:
                logger.info(f'Skipping document {doc_id}, reason: Document too short ({record.content_length} bytes)')
                continue

            if record.content_length < 500:
                logger.info(f'Skipping document {doc_id}, reason: Document too short ({record.content_length} bytes)')
                continue

            doc_id = record.headers.get('WARC-TREC-ID', record.headers.get('WARC-Record-ID'))
            webis_uuid = lib.webis_uuid(self.doc_id_prefix, doc_id)
            content_bytes = record.reader.read()

            try:
                meta = self.create_metadata(warc_name, record, content_bytes)
                payload = self.create_payload(meta, content_bytes)
                yield webis_uuid, (meta, payload)
            except SkipRecord as reason:
                logger.info(f'Skipping document {doc_id}, reason: {reason}')

    def create_metadata(self, warc_name: str, warc_record: WarcRecord, content_bytes: bytes):
        """
        Parse WARC record into header dict and decoded content.

        :param warc_name: WARC file name
        :param warc_record: WarcRecord instance (unconsumed, but with parsed HTTP)
        :param content_bytes: WarcRecord payload data as bytes
        :return: tuple of doc_uuid, (meta data, payload)
        """

        http_content_length = warc_record.content_length
        encoding = warc_record.http_charset or detect_encoding(content_bytes)

        meta = {
            'source_file': warc_name,
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
        html_tree = HTMLTree.parse(content_bytes)

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
        try:
            text_maker = html2text.HTML2Text()
            text_maker.ignore_links = True
            plain_text = text_maker.handle(str(html_tree.body))
        except:
            pass

        if len(plain_text) < 200:
            raise SkipRecord(f'Document too short ({len(plain_text)} codepoints)')

        index_doc.update({
            f'title_lang_{lang}': lib.get_document_title(html_tree),
            'meta_keywords': lib.get_document_meta_keywords(html_tree),
            f'meta_desc_lang_{lang}': lib.get_document_meta_desc(html_tree),
            f'body_lang_{lang}': plain_text,
            f'full_body_lang_{lang}': re.sub(r'\s{2,}', ' ', lib.get_full_body_text_content(html_tree)),
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
