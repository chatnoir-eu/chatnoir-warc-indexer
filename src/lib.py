from calendar import monthrange
import os
import uuid
from glob import glob
import re
import tempfile
from zipfile import ZipFile

import boto3
from elasticsearch_dsl import connections
from pyspark import SparkConf, SparkContext


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


def init_es_connection():
    """
    Initialize default Elasticsearch connection.
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
