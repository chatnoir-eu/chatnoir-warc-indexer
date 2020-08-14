import json
import os
import uuid
from glob import glob

import boto3
from elasticsearch_dsl import connections
from pyspark import SparkConf, SparkContext

_CONFIG_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'conf'))
_CONFIG = {}


def load_properties_file(filename):
    """
    Load config properties file.

    :param filename: file name (without .json or .local.json extension)
    :return: config dict
    """
    properties = json.load(open(os.path.join(_CONFIG_DIR, '{}.json'.format(filename)), 'r'))
    if os.path.isfile(os.path.join(_CONFIG_DIR, '{}.local.json'.format(filename))):
        properties.update(json.load(open(os.path.join(_CONFIG_DIR, '{}.local.json'.format(filename)), 'r')))
    return properties


def get_config(config_file=None):
    """
    Load application configuration.

    :param config_file: alternative config file
    """
    if not _CONFIG:
        _CONFIG.update(load_properties_file('config'))

    if config_file is not None:
        _CONFIG.update(json.load(open(config_file, 'r')))

    return _CONFIG


def init_es_connection():
    """
    Initialize default Elasticsearch connection.
    """
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
    if 'SPARK_HOME' not in os.environ:
        raise RuntimeError('SPARK_HOME is not set!')

    conf = SparkConf().setAppName('ChatNoir WARC Indexer').setAll(get_config()["spark"].items())
    sc = SparkContext(conf=conf)
    for py in glob(os.path.join(os.path.dirname(__file__), '*.py')):
        sc.addPyFile(py)
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
