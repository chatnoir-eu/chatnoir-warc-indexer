import logging
import time
from typing import Dict

import apache_beam as beam
import apache_beam.coders as coders
import apache_beam.transforms.userstate as userstate
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import BulkIndexError, streaming_bulk


logger = logging.getLogger()


# noinspection PyAbstractClass
class ElasticSearchBulkSink(beam.DoFn):
    BUFFER_STATE = userstate.BagStateSpec('buffer', coders.PickleCoder())
    BUFFER_COUNT_STATE = userstate.CombiningValueStateSpec('buffer_count', coders.VarIntCoder(), sum)
    EXPIRY_TIMER = userstate.TimerSpec('expiry', userstate.TimeDomain.WATERMARK)
    FLUSH_TIMER = userstate.TimerSpec('flush', userstate.TimeDomain.REAL_TIME)

    def __init__(self, es_args, chunk_size=500, max_buffer_duration=60,
                 max_retries=10, initial_backoff=2, max_backoff=600, request_timeout=120):
        super().__init__()

        self.chunk_size = chunk_size
        self.max_buffer_duration = max_buffer_duration

        self.client = get_client(**es_args)
        self.bulk_args = dict(
            chunk_size=self.chunk_size,
            raise_on_exception=False,
            raise_on_error=False,
            max_retries=0,
            request_timeout=request_timeout,
            yield_ok=True
        )
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff

        self.busy = False

    # noinspection PyMethodOverriding
    def process(self, element,
                window=beam.DoFn.WindowParam,
                buffer=beam.DoFn.StateParam(BUFFER_STATE),
                count=beam.DoFn.StateParam(BUFFER_COUNT_STATE),
                expiry_timer=beam.DoFn.TimerParam(EXPIRY_TIMER),
                stale_timer=beam.DoFn.TimerParam(FLUSH_TIMER)):

        # Reset expiration timer
        expiry_timer.set(window.end + self.max_buffer_duration)
        stale_timer.set(time.time() + self.max_buffer_duration)

        buffer.add(element)
        count.add(element)

        if count.read() >= self.chunk_size:
            yield from self._index(buffer, count)

    @userstate.on_timer(EXPIRY_TIMER)
    def expiry(self, buffer=beam.DoFn.StateParam(BUFFER_STATE), count=beam.DoFn.StateParam(BUFFER_COUNT_STATE)):
        yield from self._index(buffer, count)

    @userstate.on_timer(FLUSH_TIMER)
    def flush(self, buffer=beam.DoFn.StateParam(BUFFER_STATE), count=beam.DoFn.StateParam(BUFFER_COUNT_STATE)):
        yield from self._index(buffer, count)

    def _index(self, buffer, count):
        retry = 1
        errors = []
        batch = buffer.read()
        buffer.clear()
        count.clear()

        while retry <= self.max_retries:
            try:
                errors = []
                to_retry = []

                # We are retrying failed documents already, so don't let streaming_bulk retry
                # HTTP 429 errors, since that would mess with the result order.
                for i, (ok, info) in enumerate(streaming_bulk(self.client, batch, **self.bulk_args)):
                    if not ok:
                        to_retry.append(batch[i])
                        errors.append(info)
                    else:
                        yield info

                if not to_retry:
                    return

                batch = to_retry

            except TransportError as e:
                logger.error(f'Unexpected transport error (attempt {retry}/{self.max_retries}).')
                logger.error(e)
                if retry >= self.max_retries:
                    raise e
            else:
                logger.error(f'{len(errors)} documents failed to index (attempt {retry}/{self.max_retries})')
                logger.error('Errors: {}'.format(errors))

            time.sleep(min(self.max_backoff, self.initial_backoff * 2 ** (retry - 1)))
            retry += 1

        raise BulkIndexError(f'{len(errors)} documents failed to index.', errors)

    def teardown(self):
        self.client.transport.close()


__client = None


def get_client(**conn_args):
    global __client
    if __client is None:
        __client = Elasticsearch(**conn_args)
    return __client


def index_action(doc_id: str, index: str, data: Dict[str, str]):
    return {
        '_op_type': 'index',
        '_index': index,
        '_id': doc_id,
        **data
    }


def ensure_index(client: Elasticsearch, name: str, index_settings: Dict[str, str] = None,
                 mapping: Dict[str, str] = None):
    index_settings = index_settings or {}
    mapping = mapping or {}

    if not client.indices.exists(index=name):
        client.indices.create(index=name, body=dict(
            settings=index_settings,
            mappings=mapping
        ))
