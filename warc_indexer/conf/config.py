CONFIG = dict(
    # Elasticsearch client options
    elasticsearch=dict(
        hosts=['localhost:9200'],
        use_ssl=True,
        timeout=240,
        retry_on_timeout=True
    ),

    # Apache Beam pipeline options
    pipeline_opts=dict(
        flink_master='localhost:8081',
        environment_type='LOOPBACK',
        s3_endpoint_url='your S3 endpoint',
        s3_access_key_id='your access key',
        s3_secret_access_key='your secret key',
    ),

    # Redis WARC name cache opts
    # redis_cache=dict(
    #     host='localhost',
    #     port=6379,
    #     password=None,
    #     db=0,
    # ),
    # redis_lookup=dict(
    #     host='localhost',
    #     port=6379,
    #     password=None,
    #     db=1,
    # )
)


_CONFIG = None


def get_config():
    """
    Load application configuration.
    """
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = CONFIG.copy()
        try:
            import warc_indexer.conf.local_config
            _CONFIG.update(warc_indexer.conf.local_config.CONFIG)
        except ImportError:
            raise RuntimeError("Could not find conf.local_config.py.")

    return _CONFIG
