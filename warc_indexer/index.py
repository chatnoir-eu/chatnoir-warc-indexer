#!/usr/bin/env python3
#
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

import json
import logging
import os
import sys
from time import monotonic
import uuid
import redis

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
import click
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from fastwarc.warc import WarcRecordType
from tqdm import tqdm

from warc_indexer.conf.config import get_config
from warc_indexer.indexer.es_sink import ElasticsearchBulkSink, ensure_index
from warc_indexer.indexer.process import ProcessRecords, AddToRedisHash, map_id_val, map_val_id
from warc_indexer.indexer.warcio import ReadWarcs
from warc_indexer.indexer.textio import UnfusedReadFromText


logger = logging.getLogger()
PATH = os.path.dirname(__file__)


@click.group()
def main():
    pass


def uuid_prefix_partitioner(key, num_partitions):
    return uuid.UUID(key).int * num_partitions // pow(16, 32)


@main.command()
@click.argument('meta-index')
@click.argument('data-index')
@click.option('--shards-meta', help='Number of shards for meta index', type=int, default=10, show_default=True)
@click.option('--shards-data', help='Number of shards for data index', type=int, default=40, show_default=True)
@click.option('--replicas', help='Number of replicas for indexing (should be 0 or 1)',
              type=int, default=0, show_default=True)
def index_setup(meta_index, data_index, shards_meta, shards_data, replicas):
    """
    Set up meta data and data indices if they don't already exist.
    """
    click.echo('Setting up indices if the do not exist.')

    import warc_indexer.conf.meta_index as meta_index_conf
    import warc_indexer.conf.data_index as data_index_conf

    meta_index_conf.SETTINGS.update(dict(number_of_shards=shards_meta, number_of_replicas=replicas))
    data_index_conf.SETTINGS.update(dict(number_of_shards=shards_data, number_of_replicas=replicas))

    try:
        es_client = Elasticsearch(**get_config()['elasticsearch'])
        ensure_index(es_client, data_index, data_index_conf.SETTINGS, data_index_conf.MAPPING)
        ensure_index(es_client, meta_index, meta_index_conf.SETTINGS, meta_index_conf.MAPPING)
    except TransportError as e:
        click.echo(f'ERROR: {e.error}', err=True)
        if len(e.args) > 2:
            click.echo(str(e), err=True)


def _get_pipeline_options():
    opt_dict = dict(
        runner='FlinkRunner',
        setup_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'setup.py'),
        environment_type='LOOPBACK',
    )
    opt_dict.update(get_config()['pipeline_opts'])
    return PipelineOptions(**opt_dict)


def _get_redis_cache_prefix(base_prefix, idx_name):
    return ''.join((base_prefix, idx_name, '_CACHE_'))


def _get_redis_lookup_prefix(base_prefix, idx_name):
    return ''.join((base_prefix, idx_name, '_LOOKUP_'))


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('input-glob')
@click.argument('meta-index')
@click.argument('data-index')
@click.argument('id-prefix')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('-l', '--with-lookup', help='Look up additional pre-indexed data from Redis', is_flag=True)
@click.option('--skip-processed', help='Skip previously processed WARCs if found in Redis cache', is_flag=True)
@click.option('-p', '--index-parallelism', type=int,
              help='Indexing parallelism (same as processing parallelism if unset, will cause a reshuffle)')
@click.option('-s', '--max-content-length', type=int, default=1024 * 1024, show_default=True,
              help='Maximum record Content-Length in bytes')
@click.option('--always-index-meta', is_flag=True, help='Index metadata even if document is skipped')
@click.option('--quirks-mode', is_flag=True, help='Enable WARC quirks mode (mainly for ClueWeb09)')
@click.option('--redis-prefix', help='Redis key prefix if WARC name caching is configured',
              default='ChatNoirIndexer_WARC_', show_default=True)
@click.option('-n', '--dry-run', help='Run pipeline, but do not actually index anything.', is_flag=True)
@click.option('-o', '--additional-only', help='Index only additional data, not payloads.', is_flag=True)
def index(input_glob, meta_index, data_index, id_prefix, beam_args, **kwargs):
    """
    Index WARC contents.

    WARC records from``INPUT_GLOB`` will be index to index ``DATA_INDEX`` with WARC metadata
    and offsets indexed to ``META_INDEX``.

    ``ID_PREFIX`` is used for calculating document UUIDs.

    Additional data such as spam ranks and page ranks are indexed after indexing the original as updates
    to avid shuffling the whole payload document. If you want to index only the additional data and skip
    document payloads, specify ``--additional-only``. The WARC inputs are still needed for deriving
    the document's time-based UUIDs.
    """

    sys.argv[1:] = beam_args
    options = _get_pipeline_options()

    index_parallelism = min(1, kwargs['index_parallelism'] // 2) if kwargs['index_parallelism'] else None
    warc_args = dict(
        record_types=int(WarcRecordType.response),
        strict_mode=not kwargs['quirks_mode'],
        max_content_length=kwargs['max_content_length']
    )
    redis_cache_prefix = _get_redis_cache_prefix(kwargs['redis_prefix'], meta_index)
    redis_cache_host = get_config().get('redis') if kwargs['skip_processed'] and not kwargs['dry_run'] else None
    redis_lookup_prefix = _get_redis_lookup_prefix(kwargs['redis_prefix'], data_index)
    redis_lookup_host = get_config().get('redis') if kwargs['with_lookup'] else None

    click.echo(f'Starting pipeline to index "{input_glob}"...')
    start = monotonic()

    if kwargs['with_lookup'] and not get_config()['redis']:
        click.echo('Redis host not configured', err=True)
        sys.exit(1)

    with beam.Pipeline(options=options) as pipeline:
        # Index main metadata and payload documents
        meta, payload = (
                pipeline
                | 'Iterate WARCs' >> ReadWarcs(input_glob,
                                               warc_args=warc_args,
                                               freeze=True,
                                               overly_long_keep_meta=kwargs['always_index_meta'],
                                               redis_host=redis_cache_host,
                                               redis_prefix=redis_cache_prefix)
                | beam.WindowInto(window.FixedWindows(30))
                | 'Process Records' >> ProcessRecords(id_prefix, meta_index, data_index,
                                                      max_payload_size=kwargs['max_content_length'],
                                                      always_index_meta=kwargs['always_index_meta'],
                                                      trust_http_content_type=kwargs['quirks_mode'],
                                                      redis_lookup_host=redis_lookup_host,
                                                      redis_prefix=redis_lookup_prefix)
        )

        dry_run = kwargs['dry_run'] or kwargs['additional_only']
        dry_run_str = ' (dry run)' if dry_run else ''

        meta |= f'Index Meta Records{dry_run_str}' >> ElasticsearchBulkSink(
            get_config()['elasticsearch'], parallelism=index_parallelism, dry_run=dry_run)
        payload |= f'Index Payload Records{dry_run_str}' >> ElasticsearchBulkSink(
            get_config()['elasticsearch'], parallelism=index_parallelism, dry_run=dry_run)

    click.echo(f'Time taken: {monotonic() - start:.2f}s')


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('meta-index')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('--spam-ranks', help='File glob with spam ranks (ClueWeb format)')
@click.option('--page-ranks', help='File glob with page ranks (ClueWeb format)')
@click.option('--redis-prefix', help='Redis key prefix if WARC name caching is configured',
              default='ChatNoirIndexer_WARC_', show_default=True)
def prepare_lookups(meta_index, beam_args, spam_ranks, page_ranks, redis_prefix):
    """
    Prepare additional data to be fed into an indexing job and persist them to Redis.

    Additional index data such as spam ranks, page ranks, anchor texts etc. that are to be stored
    alongside the main index documents need to be prepared beforehand for fast ID-based lookup
    during indexing. The lookup entries are persisted to the central Redis instance configured
    in the indexer config file.

    Data will be stored under a key derived from the given ``--redis-prefix``, ``DATA_INDEX``
    and the source document ID.
    """

    sys.argv[1:] = beam_args

    if not spam_ranks and not page_ranks:
        click.echo('At least one input source must be specified.')
        sys.exit(1)

    redis_prefix = _get_redis_lookup_prefix(redis_prefix, meta_index)
    redis_cfg = get_config()['redis']
    if not redis_cfg:
        click.echo('Redis host not configured.', err=True)
        sys.exit(1)

    click.echo(f'Preparing lookup data...')
    start = monotonic()

    with beam.Pipeline(options=_get_pipeline_options()) as pipeline:
        if spam_ranks:
            _ = (pipeline
                 | 'Read Spam Ranks' >> UnfusedReadFromText(spam_ranks)
                 | 'Map Spam Ranks' >> beam.ParDo(map_val_id, val_type=int)
                 | 'Serialize Spam Ranks' >> beam.Map(lambda e: (e[0], {'spam_rank': json.dumps(e[1])}))
                 | 'Store Spam Ranks' >> AddToRedisHash(redis_cfg, redis_prefix))

        if page_ranks:
            _ = (pipeline
                 | 'Read Page Ranks' >> UnfusedReadFromText(page_ranks)
                 | 'Map Page Ranks' >> beam.ParDo(map_id_val, val_type=float)
                 | 'Serialize Page Ranks' >> beam.Map(lambda e: (e[0], {'page_rank': json.dumps(e[1])}))
                 | 'Store Page Ranks' >> AddToRedisHash(redis_cfg, redis_prefix))

    click.echo(f'Time taken: {monotonic() - start:.2f}s')


@main.command()
@click.argument('meta-index')
@click.option('--delete', type=click.Choice(['cache', 'lookup', 'all']), help='What to delete',
              default='cache', show_default=True)
@click.option('--prefix', help='Redis key base prefix to delete', default='ChatNoirIndexer_WARC_', show_default=True)
@click.option('--host', help='Override Redis host')
@click.option('--port', help='Override Redis port')
@click.option('--batch-size', help='Scan batch size', type=int, default=500, show_default=True)
def clear_redis(meta_index, delete, prefix, host, port, batch_size):
    """Clear all WARC entries with the configured prefix and index name from the Redis cache."""
    cfg = get_config().get('redis')
    if not cfg:
        click.echo('Redis host not configured.', err=True)
        return

    if host:
        cfg['host'] = host
    if port:
        cfg['port'] = port

    if delete == 'cache':
        prefix = _get_redis_cache_prefix(prefix, meta_index)
    elif delete == 'lookup':
        prefix = _get_redis_lookup_prefix(prefix, meta_index)
    else:
        ''.join((prefix, meta_index))

    redis_client = redis.Redis(**cfg)
    count = 0
    cursor = -1
    with tqdm(desc='Deleting cache entries', unit=' entries', leave=False) as prog:
        while cursor != 0:
            cursor, keys = redis_client.scan(cursor=max(0, cursor), match=prefix + '*', count=batch_size)
            if keys:
                prog.update(len(keys))
                count += len(keys)
                redis_client.delete(*keys)
    click.echo(f'Cleared {count} cache entries.')


if __name__ == '__main__':
    main()
