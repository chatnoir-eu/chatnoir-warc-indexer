FROM webis/spark-py

COPY requirements.txt .

USER 0

RUN set -x \
    && pip3 --no-cache install -r requirements.txt

USER 185
