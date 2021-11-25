# ChatNoir WARC Indexer

Install with requirements:

```bash
python3 -m venv venv
source venv/bin/activate
python3 setup.py install
```

Set up indices if they don't exist yet:

```bash
chatnoir-index index-setup META_INDEX_NAME DATA_INDEX_NAME
```

Index data from WARC S3 bucket:

```bash
chatnoir-index index 's3://bucket/warc-glob*.warc.gz' META_INDEX_NAME DATA_INDEX_NAME ID_PREFIX
```

For more information on the parameters, run with `--help`.
