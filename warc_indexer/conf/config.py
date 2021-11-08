CONFIG = {
    'es': {
        'hosts': ['localhost:9200']
    },
    's3': {
        's3_endpoint_url': 'your S3 endpoint',
        's3_access_key_id': 'your access key',
        's3_secret_access_key': 'your secret key'
    },
    'meta_index_settings': {
        'number_of_shards': 20,
        'number_of_replicas': 0,
        'refresh_interval': -1,
        'codec': 'best_compression'
    }
}
