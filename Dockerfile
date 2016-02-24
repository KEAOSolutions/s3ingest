FROM gliderlabs/alpine:3.3

WORKDIR /app
COPY . /app

RUN apk --update add python py-pip openssl ca-certificates
RUN apk --update add --virtual build-dependencies python-dev build-base wget \
  && pip install -r requirements.txt \
  && apk del build-dependencies

CMD ["/env/bin/python", "s3ingest.py"]
