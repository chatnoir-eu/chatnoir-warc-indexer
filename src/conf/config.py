CONFIG = {
    'es': {
        'hosts': ['localhost:9200']
    },
    's3': {
        'endpoint_url': 'your S3 endpoint',
        'aws_access_key_id': 'your access key',
        'aws_secret_access_key': 'your secret key'
    },
    'spark': {
    },
    'meta_index_settings': {
        'number_of_shards': 20,
        'number_of_replicas': 0,
        'refresh_interval': -1,
        'codec': 'best_compression'
    }
}
