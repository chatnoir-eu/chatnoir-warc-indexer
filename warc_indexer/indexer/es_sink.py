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
import apache_beam.typehints.typehints as t
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import streaming_bulk


logger = logging.getLogger()


class ElasticsearchBulkSink(beam.PTransform):
    def __init__(self, es_args, buffer_size=3200, chunk_size=400, max_retries=10, initial_backoff=2,
                 max_backoff=600, request_timeout=240, ignore_persistent_errors=False):
        """
        Elasticsearch bulk indexing sink.

        :param es_args: Elasticsearch client arguments
        :param buffer_size: internal buffer size
        :param chunk_size: indexing chunk size
        :param max_retries: maximum number of retries on recoverable failures
        :param initial_backoff: initial retry backoff
        :param max_backoff: maximum retry backoff
        :param request_timeout: Elasticsearch request timeout
        :param ignore_persistent_errors: ignore errors that persist after `max_retries` and continue
        """
        super().__init__()
        self._bulk_sink = _ElasticsearchBulkSink(es_args, buffer_size, chunk_size, max_retries, initial_backoff,
                                                 max_backoff, request_timeout, ignore_persistent_errors)

    def expand(self, pcoll):
        # return pcoll | beam.CombineGlobally(self._bulk_sink).without_defaults()
        return pcoll | beam.ParDo(self._bulk_sink)


# noinspection PyAbstractClass
class _ElasticsearchBulkSink(beam.DoFn):
    def __init__(self, es_args, buffer_size, chunk_size, max_retries, initial_backoff, max_backoff,
                 request_timeout, ignore_persistent_errors):
        super().__init__()

        self.buffer_size = buffer_size
        self.buffer = []

        self.es_args = es_args
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
        self.ignore_persistent_errors = ignore_persistent_errors

    def setup(self):
        self.client = Elasticsearch(**self.es_args)

    def process(self, element, *args, **kwargs):
        self.buffer.append(element)
        if len(self.buffer) > self.buffer_size:
            self._flush_buffer()

    def finish_bundle(self):
        self._flush_buffer()

    def _flush_buffer(self):
        retry = 0
        errors = []

        self.buffer.sort(key=lambda x: x.get('_id', ''))

        while retry < self.max_retries:
            try:
                errors = []
                to_retry = []

                # We are retrying failed documents already, so don't let streaming_bulk retry
                # HTTP 429 errors, since that would mess with the result order.
                for i, (ok, info) in enumerate(streaming_bulk(self.client, self.buffer, **self.bulk_args)):
                    if not ok:
                        to_retry.append(self.buffer[i])
                        errors.append(info)

                if not to_retry:
                    return

                self.buffer = to_retry

            except TransportError as e:
                logger.error('Elasticsearch error (attempt %s/%s): %s', retry + 1, self.max_retries, e.error)
                if retry == self.max_retries - 1:
                    if not self.ignore_persistent_errors:
                        raise e
                    break
                logger.error('Retrying with exponential backoff in %s seconds...', self.initial_backoff * (2 ** retry))
            finally:
                self.buffer.clear()

            time.sleep(min(self.max_backoff, self.initial_backoff * (2 ** retry)))
            retry += 1

        logger.error('%s document(s) failed to index, giving up on batch.', len(errors))
        logger.error('Failed document(s): %s', errors)

    def teardown(self):
        if self.client:
            self.client.transport.close()


def index_action(doc_id: str, index: str, data: t.Dict[str, str]):
    return {
        '_op_type': 'index',
        '_index': index,
        '_id': doc_id,
        **data
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
