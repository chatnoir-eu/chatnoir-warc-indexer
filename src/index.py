#!/usr/bin/env python3

import logging
from functools import partial

import chardet
import click
import elasticsearch_dsl as edsl
from elasticsearch.helpers import streaming_bulk
from elasticsearch_dsl import connections
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

import lib

logger = logging.getLogger()


@click.group()
@click.option('-c', '--config', type=click.File('r'), help='Alternative configuration file')
def main(config):
    if config:
        lib.get_config(config)


@main.command()
@click.argument('s3-bucket')
@click.argument('meta-index')
@click.argument('doc-id-prefix')
def warc_offsets(s3_bucket, meta_index, doc_id_prefix):
    """
    Index offsets of WARC documents into Elasticsearch.
    """
    setup_metadata_index(meta_index)

    sc = lib.get_spark_context()
    s3 = lib.get_s3_resource()
    (sc.parallelize(o.key for o in s3.Bucket(s3_bucket).objects.all())
       .flatMap(partial(parse_warc, bucket=s3_bucket))
       .map(partial(parse_record, doc_id_prefix=doc_id_prefix, discard_content=True))
       .flatMap(partial(create_index_actions, meta_index=meta_index, content_index=None))
       .mapPartitions(index_bulk)
       .count())


def with_config(func):
    """Decorator for retaining configuration in Spark workers."""
    def configured(*args, config, **kwargs):
        lib._CONFIG.update(config)
        return func(*args, **kwargs)
    return partial(configured, config=lib.get_config())


def setup_metadata_index(index_name):
    lib.init_es_connection()
    metadata = edsl.Index(index_name)
    metadata.settings(**lib.get_config()['meta_index_settings'])

    @metadata.document
    class MetaDoc(edsl.Document):
        warc_file = edsl.Keyword()
        warc_offset = edsl.Long()
        content_length = edsl.Long()
        content_type = edsl.Keyword()
        content_encoding = edsl.Keyword()
        warc_type = edsl.Keyword()
        warc_date = edsl.Date(format='date_time_no_millis')
        warc_record_id = edsl.Keyword()
        warc_warc_info_id = edsl.Keyword()
        warc_concurrent_to = edsl.Keyword()
        warc_ip_address = edsl.Ip()
        warc_target_uri = edsl.Keyword()
        warc_payload_digest = edsl.Keyword()
        warc_block_digest = edsl.Keyword()

    MetaDoc.init()


@with_config
def parse_warc(obj_name, bucket):
    warc = lib.get_s3_resource().Object(bucket, obj_name).get()['Body']
    iterator = ArchiveIterator(warc)
    for record in iterator:
        if record.rec_type != 'response':
            continue

        # Create copy without unpicklable BytesIO stream
        record_copy = ArcWarcRecord(
            record.format, record.rec_type, record.rec_headers, None,
            record.http_headers, record.content_type, record.length
        )
        record_copy.content = record.content_stream().read()

        yield obj_name, iterator.offset, record_copy


@with_config
def parse_record(warc_triple, doc_id_prefix, discard_content=False):
    """
    Parse WARC record into header dict and decoded content.
    
    :param warc_triple: triple of WARC filename, offset, record
    :param doc_id_prefix: prefix for generating Webis UUIDs
    :param discard_content: do not return WARC content (useful for metadata-only indexing)
    :return: tuple of meta data and content
    """
    warc_file, warc_offset, warc_record = warc_triple

    content = warc_record.content
    content_len = len(content)

    # Try to detect content encoding
    content_type = warc_record.http_headers.get_header('Content-Type', '').split(';')[0].strip()
    encoding = None
    if content_type.startswith('text/'):
        http_enc = warc_record.http_headers.get_header('Content-Type').split(';')
        if len(http_enc) > 1 and 'charset=' in http_enc[1]:
            encoding = http_enc[1].replace('charset=', '').strip()
        else:
            encoding = chardet.detect(content).get('encoding')
        if encoding is not None and not discard_content:
            content = content.decode(encoding, errors='ignore')

    meta = {
        'warc_file': warc_file,
        'warc_offset': warc_offset,
        'content_length': content_len,
        'content_type': content_type,
        'http_length': int(warc_record.rec_headers.get_header('Content-Length')),
        'content_encoding': encoding.lower() if encoding is not None else None,
        **{h.replace('-', '_').lower(): v for h, v in warc_record.rec_headers.headers if h.startswith('WARC-')}
    }

    doc_id = meta['warc_trec_id'] if 'warc_trec_id' in meta else meta['warc_record_id']
    return str(lib.get_webis_uuid(doc_id_prefix, doc_id)), meta, content if not discard_content else None


@with_config
def create_index_actions(data_tuple, meta_index, content_index):
    doc_id, meta, content = data_tuple
    if meta:
        yield {
            '_op_type': 'index',
            '_index': meta_index,
            '_id': doc_id,
            **meta
        }
    if content:
        yield {
            '_op_type': 'index',
            '_index': content_index,
            '_id': doc_id,
            **content
        }


@with_config
def index_bulk(actions_partition):
    lib.init_es_connection()
    yield from streaming_bulk(edsl.connections.get_connection(), actions_partition)


if __name__ == '__main__':
    main()
