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

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions
import click
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from fastwarc.warc import WarcRecordType

from warc_indexer.conf.config import get_config
from warc_indexer.indexer.es_sink import ElasticsearchBulkSink, ensure_index
from warc_indexer.indexer.process import ProcessRecord
from warc_indexer.indexer.warc_source import WarcSource


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
        es_client = Elasticsearch(**get_config()['elasticsearch'])
        ensure_index(es_client, data_index, conf.data_index.SETTINGS, conf.data_index.MAPPING)
        ensure_index(es_client, meta_index, conf.meta_index.SETTINGS, conf.meta_index.MAPPING)
    except TransportError as e:
        click.echo(f'ERROR: {e.error}', err=True)
        if len(e.args) > 2:
            click.echo(e.args[2]["error"]["root_cause"][0]["reason"], err=True)


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
    opt_dict = dict(
        runner='FlinkRunner',
        setup_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'setup.py'),
        environment_type='LOOPBACK',
    )
    opt_dict.update(get_config()['pipeline_opts'])
    options = PipelineOptions(**opt_dict)

    click.echo(f'Starting pipeline to index "{input_glob}"...')
    start = monotonic()
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Iterate WARCs' >> WarcSource(input_glob, warc_args=dict(record_types=int(WarcRecordType.response)))
            | 'Window' >> beam.WindowInto(window.FixedWindows(20))
            | 'Process Records' >> beam.ParDo(ProcessRecord(id_prefix, meta_index, data_index))
            | 'Flatten Index Actions' >> beam.FlatMap(lambda e: e[1])
            | 'Index Records' >> ElasticsearchBulkSink(get_config()['elasticsearch'])
        )
    click.echo(f'Time taken: {monotonic() - start:.2f}s')


if __name__ == '__main__':
    main()
