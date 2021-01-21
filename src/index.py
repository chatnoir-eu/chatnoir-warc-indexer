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

from bs4 import BeautifulSoup
import requests
import html2text
import sys
import langid
from urllib.parse import urlparse

import fasttext

logger = logging.getLogger()


@click.group()
def main():
    pass

def embed_fasttext(text):
    global ft
    if ft is None:
        # Load fasttext model
        ft = fasttext.load_model('models/crawl-300d-2M-subword.bin')
        print('Dimension of fasttext', ft.get_dimension())
    else:
        vectors = ft.get_word_vector(str(text))
        return vectors.tolist()

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
    # print(file_list)
    logger.info('Retrieved file list.')

    # Avoid last batch being smaller than 0.5 * batch_size and append remainder to previous batch instead
    num_batches = int(len(file_list) / batch_size + 0.5)

    for i in range(num_batches):
        logger.info('Starting batch {} of {}...'.format(i, num_batches))

        slice_start = i * batch_size
        slice_end = slice_start + batch_size if i + 1 < num_batches else len(file_list)

        (sc.parallelize(file_list[slice_start:slice_end], numSlices=slice_end - slice_start)
         .flatMap(partial(parse_warc, doc_id_prefix=doc_id_prefix, bucket=s3_bucket))
         .map(partial(parse_record, discard_content=False), preservesPartitioning=True)
         .map(parse_contents, preservesPartitioning=True)
         .flatMap(partial(create_index_actions, meta_index=meta_index, content_index=content_index), preservesPartitioning=True)
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
        number_of_replicas= 1,
        number_of_shards = 40
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
            dynamic_templates = edsl.ContentField([{
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
            encoding = t[1].lower().replace('charset=', '').strip()

    # Fallback: try to detect content encoding
    if encoding is None:
        encoding = chardet.detect(content).get('encoding')

    if encoding:
        try:
            content = content.decode(encoding, errors='ignore')
        except LookupError:
            encoding = None

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

def parse_contents(data_tuple):
    # Parse the contents of the http_content after decoding
    doc_uuid, (meta, content) = data_tuple

    # If size of content is >1MB, skip the record
    if (sys.getsizeof(content) < 1000000):
        content_soup = BeautifulSoup(content, 'lxml')
        content_ = {}

        # Extracting plain text from html body
        # https://github.com/Alir3z4/html2text/
        try:
            text_maker = html2text.HTML2Text()
            text_maker.ignore_links = True
            plain_text = text_maker.handle(str(content_soup.get_text()))
        except:
            plain_text = ""

        # Language detection from plain text - https://github.com/saffsd/langid.py
        try:
            lang = langid.classify(plain_text)[0]
        except:
            lang = "en" # Check! What should be default

        content_['full_body_lang'+ '.' + lang] = str(content)

        # Plain text and embedding
        content_['body_lang' + '.' + lang] = plain_text
        content_['body_embedding'] = embed_fasttext(plain_text)

        # Title and title embedding
        if content_soup.title == None:
            content_['title_lang'+ '.' + lang] = ""
            content_['title_embedding'] = embed_fasttext("")
        else:
            content_['title_lang'+ '.' + lang] = str(content_soup.title.string)
            content_['title_embedding'] = embed_fasttext(str(content_soup.title.string))

        # Body length
        try:
            content_['body_length'] = len(content_.get('body_lang' + '.' + lang))
        except:
            content_['body_length'] = 0

        # Page quality (between 0 to 1) from GPT2 model running locally on port 8000
        try:
            request_page_quality = requests.get(url="http://localhost:8000", data={'q': content_.get('body_lang'+'.'+lang)})
            content_['page_quality'] = request_page_quality.json().get('real_probability')
        except:
            content_['page_quality'] = 0.0

        # Meta keywords
        try:
            meta_tags = content_soup.find_all('meta')
            for tag in meta_tags:
                if 'name' in tag.attrs.keys() and tag.attrs['name'].strip().lower() in ['keywords']:
                    content_['meta_keywords'] = str(tag.attrs['content'])
                else:
                    content_['meta_keywords'] = ""
        except:
            content_['meta_keywords'] = ""

        # Meta descriptions
        try:
            meta_tags = content_soup.find_all('meta')
            for tag in meta_tags:
                if 'name' in tag.attrs.keys() and tag.attrs['name'].strip().lower() in ['descriptions']:
                    content_['meta_desc_lang'+'.'+lang] = str(tag.attrs['content'])
                else:
                    content_['meta_desc_lang'+'.'+lang] = ""
        except:
            content_['meta_desc_lang'+'.'+lang] = ""

        # Spam and page rank - dummy field
        content_['spam_rank'] = 0.0
        content_['page_rank'] = 0.0

        # Document date is same as warc date. Read from meta
        content_['date'] = meta.get('warc_date')

        # URLs
        try:
            # warc URL
            content_['warc_target_uri'] = meta.get('warc_target_uri')

            # url hostname and path
            parse_url = urlparse(content_['warc_target_uri'])
            content_['warc_target_hostname'] = parse_url.hostname
            content_['warc_target_path'] = parse_url.path
        except:
            content_['warc_target_uri'] = ""
            content_['warc_target_hostname'] = ""
            content_['warc_target_path'] = ""

        # Headings
        try:
            headings = []
            for heading in content_soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
                headings.append(heading.text.strip())
            content_['headings_lang'+'.'+lang] = headings
        except:
            content_['headings_lang' + '.' + lang] = ""

        return doc_uuid, (meta, content_)

    else:
        # Skip the record if the size is >1MB returning empty
        return doc_uuid, (meta, {})

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
