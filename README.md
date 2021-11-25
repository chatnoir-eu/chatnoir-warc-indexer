# ChatNoir WARC Indexer

Install with requirements:

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -e .
```

## 1. Configure

Create local config file and change access credentials to Elasticsearch and your Flink cluster:
```bash
cp warc_indexer/conf/config.py warc_indexer/conf/local_config.py

# Adjust values in local_config.py
```

## 2. Set up indices
Set up indices if they don't exist yet:

```bash
chatnoir-index index-setup META_INDEX_NAME DATA_INDEX_NAME
```

## 3. Index
Index data from WARC S3 bucket:

```bash
chatnoir-index index 's3://bucket/warc-glob*.warc.gz' META_INDEX_NAME DATA_INDEX_NAME ID_PREFIX
```
In addition to configuring them in `local_config.py`, you can pass any Apache Beam args (such as `--flink_master`) to the `index` command as well.

For more information on the main parameters of `chatnoir-index` or any of its subcommands, run with `--help`.
