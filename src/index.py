#!/usr/bin/env python3

from functools import partial
import logging
import uuid

import chardet
import click
import elasticsearch_dsl as edsl
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

import lib

logger = logging.getLogger()


@click.group()
def main():
    pass


def uuid_prefix_partitioner(key, num_partitions):
    return uuid.UUID(key).int * num_partitions // pow(16, 32)


@main.command()
@click.argument('s3-bucket')
@click.argument('meta-index')
@click.argument('doc-id-prefix')
@click.option('-b', '--batch-size', type=int, default=1000, show_default=True,
              help='Number of input files to process in one batch (last batch may be larger by up to 50%).')
@click.option('-f', '--path-filter', type=str, default='', help='Input path prefix filter')
@click.option('-c', '--chunk-size', type=int, default=400, show_default=True, help='Chunk size of documents to index')
@click.option('-p', '--index-parallelism', type=int, default=130, show_default=True,
              help='Number of partitions for indexing (should be smaller or equal number of workers)')
def warc_offsets(s3_bucket, meta_index, doc_id_prefix, batch_size, path_filter, chunk_size, index_parallelism):
    """
    Index offsets of WARC documents into Elasticsearch.
    """
    setup_metadata_index(meta_index)

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
         .flatMap(partial(parse_warc, doc_id_prefix=doc_id_prefix, bucket=s3_bucket))
         .map(partial(parse_record, discard_content=True), preservesPartitioning=True)
         .flatMap(partial(create_index_actions, meta_index=meta_index, content_index=None), preservesPartitioning=True)
         .cache()
         .repartitionAndSortWithinPartitions(index_parallelism,
                                             partial(uuid_prefix_partitioner, num_partitions=sc.defaultParallelism))
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


def parse_warc(obj_name, doc_id_prefix, bucket):
    """
    Iterate WARC and extract records.

    :param obj_name: input WARC file
    :param doc_id_prefix: prefix for generating Webis record UUIDs
    :param bucket: source S3 bucket
    :return: tuple of doc_uuid, (obj_name, offset, record)
    """
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

        doc_id = record_copy.rec_headers.get_header('WARC-TREC-ID',
                                                    record_copy.rec_headers.get_header('WARC-Record-ID'))

        yield str(lib.get_webis_uuid(doc_id_prefix, doc_id)), (obj_name, iterator.offset, record_copy)


def parse_record(warc_tuple, discard_content=False):
    """
    Parse WARC record into header dict and decoded content.
    
    :param warc_tuple: triple of WARC filename, offset, record
    :param discard_content: do not return WARC content (useful for metadata-only indexing)
    :return: tuple of doc_uuid, (meta data, content)
    """
    doc_uuid, (warc_file, warc_offset,  warc_record) = warc_tuple

    content = warc_record.content
    content_type = warc_record.rec_headers.get_header('Content-Type', '')
    content_length = warc_record.length
    http_content_length = 0
    http_content_type = None
    encoding = None
    if content_type.startswith('application/http'):
        http_content_length = len(content)
        t = warc_record.http_headers.get_header('Content-Type', '').split(';', maxsplit=1)
        http_content_type = t[0].strip()
        if len(t) == 2 and (http_content_type.startswith('text/') or
                            http_content_type in ['application/xhtml+xml', 'application/json']):
            encoding = t[1].replace('charset=', '').strip()

    # Fallback: try to detect content encoding
    if encoding is None:
        encoding = chardet.detect(content).get('encoding')

    if encoding is not None and not discard_content:
        content = content.decode(encoding, errors='ignore')

    meta = {
        'source_file': warc_file,
        'source_offset': warc_offset,
        **{h.replace('-', '_').lower(): v for h, v in warc_record.rec_headers.headers if h.startswith('WARC-')},
        'content_type': content_type,
        'content_length': content_length,
        'http_content_length': http_content_length,
        'http_content_type': http_content_type,
        'content_encoding': encoding.lower() if encoding else None
    }

    if 'warc_date' in meta:
        # Fix buggy ClueWeb WARC-Date headers
        meta['warc_date'] = lib.clip_warc_date(meta['warc_date'])

    return doc_uuid, (meta, content if not discard_content else None)


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
