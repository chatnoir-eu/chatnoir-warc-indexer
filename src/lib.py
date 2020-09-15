from calendar import monthrange
import itertools
import logging
import os
import uuid
from glob import glob
import re
import tempfile
import time
from zipfile import ZipFile

import boto3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import BulkIndexError, streaming_bulk
from elasticsearch_dsl import connections
from pyspark import SparkConf, SparkContext


logger = logging.getLogger()


_CONFIG = None


def get_config():
    """
    Load application configuration.
    """
    global _CONFIG
    if _CONFIG is None:
        import conf.config
        _CONFIG = conf.config.CONFIG
        try:
            import conf.local_config
            _CONFIG.update(conf.local_config.CONFIG)
        except ImportError:
            raise RuntimeError("Could not find conf.local_config.py.")

    return _CONFIG


def create_lib_zip():
    """
    ZIP Python files and modules in source directory to a temporary file.
    """
    tmp_file = tempfile.NamedTemporaryFile(suffix='.zip')
    zip_file = ZipFile(tmp_file, 'w')

    for py_file in glob(os.path.join(os.path.dirname(__file__), '**', '*.py'), recursive=True):
        zip_file.write(py_file, arcname=os.path.relpath(py_file, os.path.dirname(__file__)))
    zip_file.close()
    return tmp_file


def create_es_client():
    """
    Create and return new Elasticsearch client and connection.
    """
    return Elasticsearch(**get_config()['es'])


def init_es_connection():
    """
    Initialize persistent default Elasticsearch DSL connection.
    """
    if 'default' not in connections.connections._conns:
        connections.configure(default={
            **get_config()['es']
        })


def get_s3_resource():
    """
    :return: configured S3 resource
    """
    return boto3.resource('s3', **get_config()['s3'])


def get_spark_context() -> SparkContext:
    """
    :return: new configured Spark context
    """
    conf = SparkConf().setAppName('ChatNoir WARC Indexer').setAll(get_config()["spark"].items())
    sc = SparkContext(conf=conf)
    py_files_zip = create_lib_zip()
    sc.addPyFile(py_files_zip.name)
    sc._tmp_pyfile_zip = py_files_zip   # Keep scope alive
    return sc


def hadoop_api(spark_context):
    """
    :param spark_context: configured Spark context object
    :return: PySpark Hadoop API
    """
    return spark_context._jvm.org.apache.hadoop


def get_webis_uuid(corpus_prefix, internal_id):
    """
    Calculate a Webis document UUID based on a corpus prefix and
    an internal (not necessarily universally unique) doc ID.

    :param corpus_prefix: corpus prefix (e.g., clueweb09, cc15, ...)
    :param internal_id: internal doc ID (e.g., clueweb09-en0044-22-32198)
    :return: Webis UUID
    """
    return uuid.uuid5(uuid.NAMESPACE_URL, ':'.join((corpus_prefix, internal_id)))


def clip_warc_date(date_val):
    """
    ClueWeb WARCs have buggy WARC-Date headers with values such as '2009-03-82T07:34:44-0700', causing indexing errors.
    This function clips the day part to the number of days a month has.

    :param date_val: potentially malformed ISO 8601 date value
    :return: fixed date if day is out of range else input unchanged
    """
    def c(y, m, d):
        return '{:02}'.format(min(int(d), monthrange(int(y), int(m))[1]))

    return re.sub(r'(\d{4})-(\d{2})-(\d+)',
                  lambda g: '{}-{}-{}'.format(g.group(1), g.group(2), c(g.group(1), g.group(2), g.group(3))), date_val)


def bulk_index_partition(partition, es, chunk_size=400, batch_size=None, max_retries=10,
                         initial_backoff=2, max_backoff=600):
    """
    Index a Spark partition in small, failure-resilient micro batches, retrying documents if needed.

    :param partition: partition iterator
    :param es: initialized and connected Elasticsearch client
    :param chunk_size: indexing chunk size
    :param batch_size: micro batch size (should be a multiple of ``chunk_size``, defaults to ``10 * chunk_size``)
    :param max_retries: maximum number of retries for each micro batch
    :param initial_backoff: initial back-off in seconds before retrying (successive attempts
                            will be powers of ``initial_backoff * 2**retry_number``
    :param max_backoff: maximum number of seconds to wait before retrying a batch
    :raise: :class:`elasticsearch.helpers.BulkIndexError` if all retries failed
    """
    if not batch_size:
        batch_size = 10 * chunk_size

    part_iter = iter(partition)
    while True:
        batch = [v for _, v in itertools.islice(part_iter, batch_size)]
        if not batch:
            break
        bulk_index_micro_batch(es, batch, chunk_size, max_retries, initial_backoff, max_backoff)


def bulk_index_micro_batch(micro_batch, es, chunk_size, max_retries=10, initial_backoff=2, max_backoff=600):
    """
    Index a single micro batch in the most resilient way possible.

    If errors occur, any failed documents will be retried, even if the error is caused by
    unavailable cluster resources (such as missing shards) or connectivity issues.
    The micro batch size should be a multiple of ``chunk_size``. It can thus be larger than
    the batch itself, but must fit into memory. Thus, if you need to index a full partition,
    use :func:`index_partition_in_batches()` instead.

    :param micro_batch: micro batch as iterable
    :param es: initialized and connected Elasticsearch client
    :param chunk_size: bulk indexing chunk size
    :param max_retries: maximum number of retries
    :param initial_backoff: initial back-off in seconds before retrying (successive attempts
                            will be powers of ``initial_backoff * 2**retry_number``
    :param max_backoff: maximum number of seconds to wait before a retry
    :raise: :class:`elasticsearch.helpers.BulkIndexError` if all retries failed
    """
    if type(micro_batch) not in (list, tuple):
        micro_batch = list(micro_batch)

    retry = 1
    to_retry = []
    errors = []

    while retry <= max_retries:
        try:
            errors = []
            # We are retrying failed documents already, so don't let streaming_bulk retry
            # HTTP 429 errors, since that would mess with the result order.
            for i, (ok, info) in enumerate(
                    streaming_bulk(es, micro_batch, max_retries=0, raise_on_error=False, raise_on_exception=False,
                                   chunk_size=chunk_size, request_timeout=60)):
                if not ok:
                    to_retry.append(micro_batch[i])
                    errors.append(info)

            if not to_retry:
                return

            micro_batch = to_retry
            to_retry = []

        except TransportError as e:
            logger.error('Unexpected transport error (attempt {}/{}).'.format(retry, max_retries))
            logger.error(e)
            if retry >= max_retries:
                raise e
        else:
            logger.error('{} documents failed to index (attempt {}/{}).'.format(len(errors), retry, max_retries))
            logger.error('Errors: {}'.format(errors))

        time.sleep(min(max_backoff, initial_backoff * 2**(retry - 1)))
        retry += 1

    if not errors:
        # This line should be unreachable, but put it here anyway, just in case.
        return

    raise BulkIndexError('{} documents failed to index.'.format(len(errors)), errors)
