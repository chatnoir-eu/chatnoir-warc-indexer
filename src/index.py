#!/usr/bin/env python3

from functools import partial
import logging
import re
import sys
from urllib.parse import urlparse
import uuid

import click
import elasticsearch_dsl as edsl
import html2text
from fastwarc.warc import ArchiveIterator, WarcRecord, WarcRecordType
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast as lang_detect_fast

import lib


logger = logging.getLogger()


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
@click.argument('s3-bucket')
@click.argument('meta-index')
@click.argument('content-index')
@click.argument('doc-id-prefix')
@click.option('-b', '--batch-size', type=int, default=1000, show_default=True,
              help='Number of input files to process in one batch (last batch may be larger by up to 50%).')
@click.option('-f', '--path-filter', type=str, default='', help='Input path prefix filter')
@click.option('-c', '--chunk-size', type=int, default=400, show_default=True, help='Chunk size of documents to index')
@click.option('-p', '--index-parallelism', type=int, default=130, show_default=True,
              help='Number of partitions for indexing (should be smaller or equal number of workers)')
def warc_offsets(s3_bucket, meta_index, content_index, doc_id_prefix, batch_size, path_filter, chunk_size, index_parallelism):
    """
    Index offsets of WARC documents into Elasticsearch.
    """
    setup_metadata_index(meta_index)
    setup_contentdata_index(content_index)

    sc = lib.get_spark_context()

    s3 = lib.get_s3_resource()
    logger.debug('Retrieving file list...')
    file_list = list(o.key for o in s3.Bucket(s3_bucket).objects.filter(Prefix=path_filter))

    logger.info('Retrieved file list.')

    # Avoid last batch being smaller than 0.5 * batch_size and append remainder to previous batch instead
    num_batches = int(len(file_list) / batch_size + 0.5)

    for i in range(num_batches):
        logger.info('Starting batch {} of {}...'.format(i, num_batches))

        slice_start = i * batch_size
        slice_end = slice_start + batch_size if i + 1 < num_batches else len(file_list)

        (sc.parallelize(file_list[slice_start:slice_end], numSlices=slice_end - slice_start)
         .flatMap(partial(parse_warc_stream, doc_id_prefix=doc_id_prefix, bucket=s3_bucket))
         .map(partial(parse_record, discard_content=False), preservesPartitioning=True)
         .flatMap(partial(create_index_actions, meta_index=meta_index, content_index=content_index),
                  preservesPartitioning=True)
         .repartitionAndSortWithinPartitions(index_parallelism,
                                             partial(uuid_prefix_partitioner, num_partitions=index_parallelism))
         .cache()
         .foreachPartition(partial(index_partition, chunk_size=chunk_size)))

        logger.info('Completed batch {}.'.format(i))


def setup_metadata_index(index_name):
    lib.init_es_connection()
    metadata = edsl.Index(index_name)

    @metadata.document
    class MetaDoc(edsl.Document):
        source_file = edsl.Keyword()
        source_offset = edsl.Long()
        content_length = edsl.Long()
        content_type = edsl.Keyword()
        content_encoding = edsl.Keyword()
        http_content_length = edsl.Long()
        http_content_type = edsl.Keyword()
        warc_type = edsl.Keyword()
        warc_date = edsl.Date(format='date_time_no_millis')
        warc_record_id = edsl.Keyword()
        warc_trec_id = edsl.Keyword()
        warc_warcinfo_id = edsl.Keyword()
        warc_concurrent_to = edsl.Keyword()
        warc_truncated = edsl.Keyword()
        warc_ip_address = edsl.Ip()
        warc_target_uri = edsl.Keyword()
        warc_identified_payload_type = edsl.Keyword()
        warc_payload_digest = edsl.Keyword()
        warc_block_digest = edsl.Keyword()

        class Meta:
            dynamic_templates = edsl.MetaField([{
                'additional_warc_headers': {
                    'match_mapping_type': 'string',
                    'match': 'warc_*',
                    'mapping': {
                        'type': 'keyword'
                    }
                }
            }])

    if not metadata.exists():
        metadata.settings(**lib.get_config()['meta_index_settings'])
        MetaDoc.init()


# Setup content data index
def setup_contentdata_index(index_name):
    lib.init_es_connection()
    contentdata = edsl.Index(index_name)

    # Set number of replicas to 1
    # Set number of shards to 40
    contentdata.settings(
        number_of_replicas=1,
        number_of_shards=40
    )

    @contentdata.document
    class ContentDoc(edsl.Document):

        # Contents
        body_length = edsl.Long()
        page_quality = edsl.Long()
        spam_rank = edsl.Long()
        page_rank = edsl.Long()
        meta_keywords = edsl.Keyword()
        anchor_texts = edsl.Keyword()
        date = edsl.Date(format='date_time_no_millis')
        warc_target_hostname = edsl.Keyword()
        warc_target_path = edsl.Keyword()
        title_embedding = edsl.DenseVector(300)
        body_embedding = edsl.DenseVector(300)

        class Content:
            dynamic_templates = edsl.MetaField([{
                'additional_warc_headers': {
                    'match_mapping_type': 'string',
                    'match': 'warc_*',
                    'mapping': {
                        'type': 'keyword'
                    }
                }
            }])

    if not contentdata.exists():
        contentdata.settings(**lib.get_config()['content_index_settings'])
        ContentDoc.init()


class SkipRecord(Exception):
    pass


def parse_warc_stream(bucket_name, obj_name: str, doc_id_prefix: str, bucket: str):
    """
    Iterate WARC and extract records.

    :param bucket_name: input WARC file
    :param obj_name: input WARC file
    :param doc_id_prefix: prefix for generating Webis record UUIDs
    :param bucket: source S3 bucket
    :return: tuple of doc_uuid, (obj_name, offset, record)
    """
    if not obj_name.endswith('.warc.gz'):
        logger.warning('Skipping non-WARC file {}'.format(obj_name))
        return []

    warc = lib.get_s3_resource().Object(bucket, obj_name).get()['Body']
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

        try:
            yield parse_record(obj_name, doc_id_prefix, record)
        except SkipRecord as reason:
            logger.info(f'Skipping document {doc_id}, reason: {reason}')


def parse_record(warc_file_name: str, doc_id_prefix: str, warc_record: WarcRecord, discard_content: bool = False):
    """
    Parse WARC record into header dict and decoded content.
    
    :param warc_file_name: name of input WARC file
    :param doc_id_prefix: prefix for generating Webis record UUIDs
    :param warc_record: WarcRecord instance (unconsumed, but with parsed HTTP)
    :param discard_content: do not return WARC content (useful for metadata-only indexing)
    :return: tuple of doc_uuid, (meta data, payload)
    """

    doc_id = warc_record.headers.get('WARC-TREC-ID', warc_record.headers.get('WARC-Record-ID'))
    webis_uuid = lib.webis_uuid(doc_id_prefix, doc_id)

    http_content_length = warc_record.content_length
    content_bytes = warc_record.reader.read()
    encoding = warc_record.http_charset or detect_encoding(content_bytes)

    meta = {
        'source_file': warc_file_name,
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

    content = None
    if not discard_content:
        content = parse_payload(meta, content_bytes, encoding)

    return webis_uuid, (meta, content)


def parse_payload(metadata, content_bytes, content_encoding):
    """
    Parse WARC record payload into an index document.

    :param metadata: WARC metadata dict
    :param content_bytes: raw payload as bytes
    :param content_encoding: content encoding
    :return: index document dict
    """

    content_str = bytes_to_str(content_bytes, content_encoding)
    lang = lang_detect_fast(content_str)
    parse_url = urlparse(metadata['warc_target_uri'])
    html_tree = HTMLTree.parse(content_bytes)

    index_doc = {
        'warc_record_id': metadata.get('warc_record_id'),
        'warc_trec_id': metadata.get('warc_trec_id'),
        'date': metadata.get('warc_date'),
        'body_length': len(content_str),
        'warc_target_uri': metadata.get('warc_target_uri'),
        'warc_target_hostname': parse_url.hostname,
        'warc_target_path': parse_url.path,
        'warc_target_query_string': parse_url.query,
        'content_type': metadata.get('http_content_type'),
    }

    # Extracting plain text from html body
    # https://github.com/Alir3z4/html2text/
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
        f'meta_desc_lang_{lang}': lib.get_document_meta_desc(html_tree),
        f'meta_keywords_lang_{lang}': lib.get_document_meta_keywords(html_tree),
        f'body_lang_{lang}': plain_text,
        f'full_body_lang_{lang}': re.sub(r'\s{2,}', ' ', lib.get_full_body_text_content(html_tree)),
    })

    # Prune keys with empty or None values
    doc_keys = list(index_doc.keys())
    for k in doc_keys:
        if not index_doc[k]:
            del index_doc[k]

    return index_doc


def create_index_actions(data_tuple, meta_index, content_index):
    doc_uuid, (meta, content) = data_tuple
    if meta:
        yield doc_uuid, {
            '_op_type': 'index',
            '_index': meta_index,
            '_id': doc_uuid,
            **meta
        }
    if content:
        yield doc_uuid, {
            '_op_type': 'index',
            '_index': content_index,
            '_id': doc_uuid,
            **content
        }


def index_partition(partition, **kwargs):
    lib.bulk_index_partition(partition, es=lib.create_es_client(), **kwargs)


if __name__ == '__main__':
    main()
