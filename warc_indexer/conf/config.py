CONFIG = dict(
    # Elasticsearch client options
    elasticsearch=dict(
        hosts=['localhost:9200'],
        use_ssl=True,
    ),

    # Apache Beam pipeline options
    pipeline_opts=dict(
        flink_master='localhost:8081',
        environment_type='LOOPBACK',
        s3_endpoint_url='your S3 endpoint',
        s3_access_key_id='your access key',
        s3_secret_access_key='your secret key',
    )
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
