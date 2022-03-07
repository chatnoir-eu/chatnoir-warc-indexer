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

import logging
import os
import sys
from time import monotonic
import uuid
import redis

import apache_beam as beam
from apache_beam.io import fileio, textio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
import click
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from fastwarc.warc import WarcRecordType

from warc_indexer.conf.config import get_config
from warc_indexer.indexer.es_sink import ElasticsearchBulkSink, ensure_index, update_action
from warc_indexer.indexer.process import MapKeysToWebisUUID, ProcessRecords, map_id_val, map_val_id
from warc_indexer.indexer.warc_source import WarcInput


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


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('input-glob')
@click.argument('meta-index')
@click.argument('data-index')
@click.argument('id-prefix')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('--spam-ranks', help='File with spam ranks (ClueWeb format)')
@click.option('--page-ranks', help='File with page ranks (ClueWeb format)')
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
    opt_dict = dict(
        runner='FlinkRunner',
        setup_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'setup.py'),
        environment_type='LOOPBACK',
    )
    opt_dict.update(get_config()['pipeline_opts'])
    options = PipelineOptions(**opt_dict)

    warc_args = dict(
        record_types=int(WarcRecordType.response),
        strict_mode=not kwargs['quirks_mode'],
        max_content_length=kwargs['max_content_length']
    )
    redis_prefix = ''.join((kwargs['redis_prefix'], meta_index, '_'))
    index_parallelism = min(1, kwargs['index_parallelism'] // 2) if kwargs['index_parallelism'] else None

    click.echo(f'Starting pipeline to index "{input_glob}"...')
    start = monotonic()

    with beam.Pipeline(options=options) as pipeline:
        # Index main metadata and payload documents
        meta, payload = (
                pipeline
                | 'Iterate WARCs' >> WarcInput(input_glob,
                                               warc_args=warc_args,
                                               freeze=True,
                                               overly_long_keep_meta=kwargs['always_index_meta'],
                                               redis_host=get_config().get('redis'),
                                               redis_prefix=redis_prefix)
                | beam.WindowInto(window.FixedWindows(30))
                | 'Process Records' >> ProcessRecords(id_prefix, meta_index, data_index,
                                                      max_payload_size=kwargs['max_content_length'],
                                                      always_index_meta=kwargs['always_index_meta'],
                                                      trust_http_content_type=kwargs['quirks_mode'])
        )

        dry_run = kwargs['dry_run'] or kwargs['additional_only']
        dry_run_str = ' (dry run)' if dry_run else ''

        meta |= f'Index Meta Records{dry_run_str}' >> ElasticsearchBulkSink(
            get_config()['elasticsearch'], parallelism=index_parallelism, dry_run=dry_run)
        payload |= f'Index Payload Records{dry_run_str}' >> ElasticsearchBulkSink(
            get_config()['elasticsearch'],
            parallelism=index_parallelism, dry_run=dry_run, retain_fields=['uuid'])

        # CoJoin additional data with index IDs after they have been indexed
        additional_inputs = {}
        if kwargs['spam_ranks']:
            additional_inputs['spam_rank'] = (pipeline
                                              | 'Match Spam Spam Inputs' >> fileio.MatchFiles(kwargs['spam_ranks'])
                                              | 'Reshuffle Spam Rank Inputs' >> beam.Reshuffle()
                                              | 'Read Spam Ranks' >> textio.ReadAllFromText()
                                              | 'Map Spam Ranks' >> beam.ParDo(map_val_id, val_type=int)
                                              | 'Map Spam Rank IDs' >> beam.ParDo(MapKeysToWebisUUID(id_prefix)))

        if kwargs['page_ranks']:
            additional_inputs['page_rank'] = (pipeline
                                              | 'Match Page Rank Inputs' >> fileio.MatchFiles(kwargs['page_ranks'])
                                              | 'Reshuffle Page Rank Inputs' >> beam.Reshuffle()
                                              | 'Read Page Ranks' >> textio.ReadAllFromText()
                                              | 'Map Page Ranks' >> beam.ParDo(map_id_val, val_type=float)
                                              | 'Map Page Rank IDs' >> beam.ParDo(MapKeysToWebisUUID(id_prefix)))

        if additional_inputs:
            # Reverse map index and Webis UUID
            additional_inputs['_id'] = payload | beam.Map(lambda e: (e[1]['uuid'], e[0]))

            def map_to_update_actions(e):
                if len(e) > 1 and '_id' in e and len(e['_id']) == 1:
                    doc_id = e['_id'][0]
                    data = {k: v[0] for k, v in e.items() if not k.startswith('_')}
                    yield update_action(doc_id, data_index, data)

            dry_run_str = ' (dry run)' if kwargs['dry_run'] else ''
            _ = (
                    additional_inputs
                    | 'CoGroup Additional Inputs' >> beam.CoGroupByKey()
                    | 'Create Update Index Actions' >> beam.FlatMap(map_to_update_actions)
                    | f'Index Additional Data{dry_run_str}' >> ElasticsearchBulkSink(
                            get_config()['elasticsearch'],
                            parallelism=index_parallelism,
                            dry_run=kwargs['dry_run'])
            )

    click.echo(f'Time taken: {monotonic() - start:.2f}s')


@main.command()
@click.option('--prefix', help='Redis key prefix to delete', default='ChatNoirIndexer_WARC_', show_default=True)
@click.option('--host', help='Override Redis host')
@click.option('--port', help='Override Redis port')
def clear_redis_cache(prefix, host, port):
    """Clear all WARC entries with the configured prefix from the Redis cache."""
    cfg = get_config().get('redis')
    if not cfg:
        click.echo('Redis host not configured.', err=True)
        return

    if host:
        cfg['host'] = host
    if port:
        cfg['port'] = port

    redis_client = redis.Redis(**cfg)
    count = 0
    for k in redis_client.scan_iter(prefix + '*'):
        redis_client.delete(k)
        count += 1
    click.echo(f'Cleared {count} cache entries.')


if __name__ == '__main__':
    main()
