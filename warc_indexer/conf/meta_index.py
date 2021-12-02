MAPPING = {
    "properties": {
        "uuid": {
            "type": "keyword"
        },
        "content_encoding": {
            "type": "keyword"
        },
        "content_length": {
            "type": "long"
        },
        "content_type": {
            "type": "keyword"
        },
        "source_file": {
            "type": "keyword"
        },
        "source_offset": {
            "type": "long"
        },
        "warc_date": {
            "type": "date",
            "format": "date_time_no_millis"
        },
        "warc_ip_address": {
            "type": "ip"
        }
    },
    "dynamic_templates": [
        {
            "warc_headers": {
                "match": "warc_*",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "keyword"
                }
            },
        },
        {
            "http_headers": {
                "match": "http_*",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "keyword"
                }
            }
        }
    ]
}

SETTINGS = {
    "codec": "best_compression",
    "refresh_interval": "-1"
}
