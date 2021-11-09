CONFIG = dict(
    # Elasticsearch client options
    elasticsearch=dict(
        hosts=['localhost:9200'],
        use_ssl=True
    ),

    # Apache Beam pipeline options
    pipeline_opts=dict(
        s3_endpoint_url='your S3 endpoint',
        s3_access_key_id='your access key',
        s3_secret_access_key='your secret key'
    )
)
