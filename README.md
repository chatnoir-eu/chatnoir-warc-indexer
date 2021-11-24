1. Create index with the desired index name in the Kibana console using the command in "create_index_kibana.txt"
2. Download the Fast text model in the folder "models" from https://fasttext.cc/docs/en/english-vectors.html
3. Run the gpt2 model as a server with "python -m detector.server detector-base.pt --port 8000"
4. Run the index.py with arguments pointing to s3-bucket, index name etc
