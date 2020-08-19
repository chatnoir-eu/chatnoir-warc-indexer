#!/usr/bin/env python3

from calendar import monthrange
from functools import partial
import logging
import re

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
def main():
    pass


@main.command()
@click.argument('s3-bucket')
@click.argument('meta-index')
@click.argument('doc-id-prefix')
@click.option('-f', '--path-filter', type=str, default='', help='Input path prefix filter')
def warc_offsets(s3_bucket, meta_index, doc_id_prefix, path_filter):
    """
    Index offsets of WARC documents into Elasticsearch.
    """
    setup_metadata_index(meta_index)

    sc = lib.get_spark_context()

    s3 = lib.get_s3_resource()
    file_list = list(o.key for o in s3.Bucket(s3_bucket).objects.filter(Prefix=path_filter))

    (sc.parallelize(file_list, numSlices=max(len(file_list), sc.defaultParallelism))
       .flatMap(partial(parse_warc, bucket=s3_bucket))
       .coalesce(sc.defaultParallelism)
       .map(partial(parse_record, doc_id_prefix=doc_id_prefix, discard_content=True))
       .flatMap(partial(create_index_actions, meta_index=meta_index, content_index=None))
       .mapPartitions(index_bulk)
       .count())


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
        http_length = edsl.Long()
        warc_type = edsl.Keyword()
        warc_date = edsl.Date(format='date_time_no_millis')
        warc_record_id = edsl.Keyword()
        warc_warc_info_id = edsl.Keyword()
        warc_concurrent_to = edsl.Keyword()
        warc_ip_address = edsl.Ip()
        warc_target_uri = edsl.Keyword()
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

    MetaDoc.init()


def parse_warc(obj_name, bucket):
    if not obj_name.endswith('.warc.gz'):
        logger.warning('Skipping non-WARC file {}'.format(obj_name))
        return []

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

    # Clueweb WARCs have buggy dates like '2009-03-82T07:34:44-0700' causing indexing errors
    if 'warc_date' in meta:
        def clip_day(y, m, d):
            return '{:02}'.format(min(int(d), monthrange(int(y), int(m))[1]))
        meta['warc_date'] = re.sub(r'(\d{4})-(\d{2})-(\d+)',
                                   lambda g: '{}-{}-{}'.format(
                                       g.group(1),
                                       g.group(2),
                                       clip_day(g.group(1), g.group(2), g.group(3))),
                                   meta['warc_date'])

    doc_id = meta['warc_trec_id'] if 'warc_trec_id' in meta else meta['warc_record_id']
    return str(lib.get_webis_uuid(doc_id_prefix, doc_id)), meta, content if not discard_content else None


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


def index_bulk(actions_partition):
    lib.init_es_connection()
    yield from streaming_bulk(edsl.connections.get_connection(), actions_partition)


if __name__ == '__main__':
    main()
