FROM gliderlabs/alpine:3.3

WORKDIR /app
COPY . /app

RUN apk --no-cache add python py-pip openssl ca-certificates
RUN apk --no-cache add --virtual build-dependencies python-dev build-base wget
RUN pip install -r requirements.txt
RUN apk del build-dependencies

CMD ["/env/bin/python", "s3ingest.py"]
