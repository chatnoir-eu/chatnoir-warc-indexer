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
import time

import apache_beam as beam
from apache_beam.utils.windowed_value import WindowedValue
import apache_beam.typehints.typehints as t
from elasticsearch import exceptions as es_exc, Elasticsearch
from elasticsearch.helpers import streaming_bulk


logger = logging.getLogger()


class ElasticsearchBulkSink(beam.PTransform):
    def __init__(self, es_args,
                 update=False,
                 parallelism=None,
                 buffer_size=3200,
                 chunk_size=800,
                 max_retries=10,
                 initial_backoff=2,
                 max_backoff=600,
                 request_timeout=240,
                 ignore_persistent_400=True,
                 dry_run=False,
                 retain_fields=None):
        """
        Elasticsearch bulk indexing sink.

        Returns the document IDs of successfully indexed documents.

        :param es_args: Elasticsearch client arguments
        :param update: do a bulk UPDATE instead of a bulk INDEX (requires documents to exist)
        :param parallelism: reshuffle to achieve the desired level of parallelism
        :param buffer_size: internal buffer size
        :param chunk_size: indexing chunk size
        :param max_retries: maximum number of retries on recoverable failures
        :param initial_backoff: initial retry backoff
        :param max_backoff: maximum retry backoff
        :param request_timeout: Elasticsearch request timeout
        :param ignore_persistent_400: ignore persistent ``RequestError``s, i.e., errors with HTTP code 400
        :param dry_run: discard documents and do not actually index them
        :param retain_fields: instead of plain index IDs, map inputs to KV pairs retaining the specified index fields
        """
        super().__init__()
        self._bulk_sink = _ElasticsearchBulkSink(es_args=es_args,
                                                 update=update,
                                                 buffer_size=buffer_size,
                                                 chunk_size=chunk_size,
                                                 max_retries=max_retries,
                                                 initial_backoff=initial_backoff,
                                                 max_backoff=max_backoff,
                                                 request_timeout=request_timeout,
                                                 ignore_persistent_400=ignore_persistent_400,
                                                 dry_run=dry_run,
                                                 retain_fields=retain_fields)
        self.parallelism = parallelism

    def expand(self, pcoll):
        if self.parallelism:
            pcoll |= beam.Reshuffle(self.parallelism)
        return pcoll | beam.ParDo(self._bulk_sink)


class _BatchAccumulator:
    def __init__(self):
        self.index_count = 0
        self.failed_count = 0
        self.buffer = []


# noinspection PyAbstractClass
class _ElasticsearchBulkSink(beam.DoFn):
    def __init__(self,
                 es_args,
                 update,
                 buffer_size,
                 chunk_size,
                 max_retries,
                 initial_backoff,
                 max_backoff,
                 request_timeout,
                 ignore_persistent_400,
                 dry_run,
                 retain_fields):
        super().__init__()

        self.buffer_size = buffer_size
        self.buffer = []

        self.es_args = es_args
        self.update = update
        self.client = None
        self.bulk_args = dict(
            chunk_size=chunk_size,
            raise_on_exception=False,
            raise_on_error=False,
            max_retries=0,
            request_timeout=request_timeout,
            yield_ok=True
        )
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.ignore_persistent_400 = ignore_persistent_400
        self.dry_run = dry_run
        self.retain_fields = set(retain_fields) if retain_fields else None

    def setup(self):
        self.client = Elasticsearch(**self.es_args)

    def teardown(self):
        assert len(self.buffer) == 0
        if self.client:
            self.client.transport.close()

    # noinspection PyIncorrectDocstring
    def process(self, element, *args, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, **kwargs):
        """
        Add element to index buffer.
        Returns an iterable of successfully index document IDs on buffer flush or ``None`` otherwise.
        The returned IDs will be KV pairs of (document UUID, index UUID).

        :param element: input index action
        :return: iterable of indexed document IDs or ``None``
        """

        if self.dry_run:
            val = element.get('_id', '')
            if self.retain_fields:
                val = val, {k: v for k, v in element.items() if k in self.retain_fields}
            yield val
            return

        self.buffer.append((element, timestamp, window))
        if len(self.buffer) >= self.buffer_size:
            yield from self._flush_buffer()

    def finish_bundle(self):
        if len(self.buffer) > 0:
            yield from self._flush_buffer()

    def _flush_buffer(self):
        retry = 0
        errors = []
        self.buffer.sort(key=lambda x: x[0].get('_id', ''))

        try:
            while retry < self.max_retries:
                try:
                    errors = []
                    to_retry = []

                    # We are retrying failed documents already, so don't let streaming_bulk retry
                    # HTTP 429 errors, since that would mess with the result order.
                    buf_gen = (e[0] for e in self.buffer)
                    for i, (ok, info) in enumerate(streaming_bulk(self.client, buf_gen, **self.bulk_args)):
                        if not ok:
                            to_retry.append(self.buffer[i])
                            errors.append(info)
                        else:
                            val = list(info.values())[0]['_id']
                            if self.retain_fields:
                                val = val, {k: v for k, v in self.buffer[i][0].items() if k in self.retain_fields}

                            yield WindowedValue(val, self.buffer[i][1], self.buffer[i][2])

                    if not to_retry:
                        return

                    self.buffer = to_retry

                except es_exc.TransportError as e:
                    logger.error('Elasticsearch error (attempt %s/%s): %s', retry + 1, self.max_retries, e.error)
                    if retry == self.max_retries - 1:
                        if not isinstance(e, es_exc.RequestError) or not self.ignore_persistent_400:
                            raise e
                        break
                    logger.error('Retrying with exponential backoff in %s seconds...',
                                 self.initial_backoff * (2 ** retry))

                time.sleep(min(self.max_backoff, self.initial_backoff * (2 ** retry)))
                retry += 1
        finally:
            self.buffer.clear()

        logger.error('%s document(s) failed to index, giving up on batch.', len(errors))
        logger.error('Failed document(s): %s', errors)


def index_action(doc_id: str, index: str, data: t.Dict[str, str]):
    """Create a bulk index action."""
    return {
        '_op_type': 'index',
        '_index': index,
        '_id': doc_id,
        **{k: v for k, v in data.items() if not k.startswith('_')}
    }


def update_action(doc_id: str, index: str, data: t.Dict[str, str]):
    """Create a bulk update action."""
    return {
        '_op_type': 'update',
        '_index': index,
        '_id': doc_id,
        'doc': {k: v for k, v in data.items() if not k.startswith('_')}
    }


def ensure_index(client: Elasticsearch, name: str, index_settings: t.Dict[str, str] = None,
                 mapping: t.Dict[str, str] = None):
    index_settings = index_settings or {}
    mapping = mapping or {}

    if not client.indices.exists(index=name):
        client.indices.create(index=name, body=dict(
            settings=index_settings,
            mappings=mapping
        ))
