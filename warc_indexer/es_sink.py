import logging
import time

import apache_beam as beam
import apache_beam.coders as coders
import apache_beam.transforms.userstate as userstate
import apache_beam.typehints.typehints as t
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import BulkIndexError, streaming_bulk


logger = logging.getLogger()


# noinspection PyAbstractClass
class ElasticSearchBulkSink(beam.DoFn):
    BUFFER_STATE = userstate.BagStateSpec('buffer', coder=coders.FastPrimitivesCoder())
    BUFFER_COUNT_STATE = userstate.CombiningValueStateSpec('buffer_count', combine_fn=sum)
    EXPIRY_TIMER = userstate.TimerSpec('expiry', userstate.TimeDomain.WATERMARK)
    FLUSH_TIMER = userstate.TimerSpec('flush', userstate.TimeDomain.REAL_TIME)

    def __init__(self, es_args, chunk_size=500, max_buffer_duration=60,
                 max_retries=10, initial_backoff=2, max_backoff=600, request_timeout=120):
        super().__init__()

        self.chunk_size = chunk_size
        self.max_buffer_duration = max_buffer_duration

        self.es_args = es_args
        self.client = None
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

    def setup(self):
        super().setup()
        self.client = Elasticsearch(**self.es_args)

    # noinspection PyMethodOverriding
    def process(self, element: t.KV[str, t.Tuple[t.Dict[str, str], t.Dict[str, str]]],
                window=beam.DoFn.WindowParam,
                ts=beam.DoFn.TimestampParam,
                buffer=beam.DoFn.StateParam(BUFFER_STATE),
                count=beam.DoFn.StateParam(BUFFER_COUNT_STATE),
                expiry_timer=beam.DoFn.TimerParam(EXPIRY_TIMER),
                stale_timer=beam.DoFn.TimerParam(FLUSH_TIMER)):

        # Reset expiration timer
        # expiry_timer.set(window.end + 1)
        # stale_timer.set(time.time() + 1)

        webis_uuid, (meta, payload) = element

        print(count.read(), window.end, ts)
        buffer.add(meta)
        buffer.add(payload)
        count.add(2)
        open('/home/roce3528/Desktop/foo', 'a').write(str(count.read()) + '\n')

        if count.read() >= self.chunk_size:
            print('sfds')
        #     yield from self._index(buffer.read())
        #     buffer.clear()
        #     count.clear()

    @userstate.on_timer(EXPIRY_TIMER)
    def expiry(self, buffer=beam.DoFn.StateParam(BUFFER_STATE), count=beam.DoFn.StateParam(BUFFER_COUNT_STATE)):
        # yield from self._index(buffer.read())
        print(count.read())
        print('expired')

        # buffer.clear()
        # count.clear()

    @userstate.on_timer(FLUSH_TIMER)
    def flush(self, buffer=beam.DoFn.StateParam(BUFFER_STATE), count=beam.DoFn.StateParam(BUFFER_COUNT_STATE)):

        print(count.read())
        print('expiredx')
        # yield from self._index(buffer.read())
        #
        # buffer.clear()
        # count.clear()

    def _index(self, batch):
        retry = 1
        errors = []
        print(len(batch))
        return

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
