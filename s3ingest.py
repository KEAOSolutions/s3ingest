#!/usr/bin/env python
import botocore
import boto3
import os
import requests
import time
import datetime

S3_BUCKET_NAME = os.getenv('S3_BUCKET', None)
S3_IN_DATA_FOLDER = os.getenv('S3_IN_DATA_FOLDER', None)
S3_PROCESSED_DATA_FOLDER = os.getenv('S3_PROCESSED_DATA_FOLDER', None)
S3_URL = os.getenv('S3_URL', None)
S3_PROXY = os.getenv('S3_PROXY', None)
S3_PROXY_SSL = os.getenv('S3_PROXY_SSL', None)
S3_REGION = os.getenv('S3_REGION', None)
S3_SSL_VERIFY = os.getenv('S3_SSL_VERIFY', True)
POST_URL = os.getenv('POST_URL', None)
POST_PATH = os.getenv('POST_PATH', '/')
POST_PROXY = os.getenv('POST_PROXY', None)
POST_PROXY_SSL = os.getenv('POST_PROXY_SSL', None)
SLEEP_TIME = os.getenv('SLEEP_TIME', '30')
DEBUG = os.getenv('DEBUG', None)

try:
    with open("/secret/AWS_ACCESS_KEY_ID", "r") as secretfile:
        AWS_ACCESS_KEY_ID = secretfile.read()
except IOError:
    print("The secret file for AWS_ACCESS_KEY_ID was not found falling back " +
          "to the environment key")
else:
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)

try:
    with open("/secret/AWS_SECRET_ACCESS_KEY", "r") as secretfile:
        AWS_SECRET_ACCESS_KEY = secretfile.read()
except IOError:
    print("The secret file for AWS_SECRET_ACCESS_KEY was not found falling " +
          "back to the environment key")
else:
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


if not S3_BUCKET_NAME:
    print("Environment variable: S3_BUCKET not set")
    raise SystemExit(2)

if not S3_IN_DATA_FOLDER:
    print("Environment variable: S3_IN_DATA_FOLDER not set")
    raise SystemExit(2)

if not S3_PROCESSED_DATA_FOLDER:
    print("Environment variable: S3_PROCESSED_DATA_FOLDER not set")
    raise SystemExit(2)

if not S3_URL:
    print("Environment variable: S3_URL not set")
    raise SystemExit(2)

if not S3_REGION:
    print("Environment variable: S3_REGION not set")
    raise SystemExit(2)

if not POST_URL:
    print("Environment variable: POST_URL not set")
    raise SystemExit(2)


print("Setting up s3 proxies")
if S3_PROXY:
    os.environ["HTTP_PROXY"] = S3_PROXY

if S3_PROXY_SSL:
    os.environ["HTTPS_PROXY"] = S3_PROXY_SSL

print("Setting up Post proxies")
if POST_PROXY and POST_PROXY_SSL:
    POST_PROXIES = {
        "http": POST_PROXY,
        "https": POST_PROXY_SSL
    }
elif POST_PROXY and not POST_PROXY_SSL:
    POST_PROXIES = {
        "http": POST_PROXY,
        "https": None
    }
elif POST_PROXY_SSL and not POST_PROXY:
    POST_PROXIES = {
        "http": None,
        "https": POST_PROXY_SSL
    }
else:
    POST_PROXIES = {
        "http": None,
        "https": None
    }


def download_file_from_s3():
    for obj in BUCKET.objects.filter(Prefix=S3_IN_DATA_FOLDER):
        filename = "/tmp/" + os.path.split(obj.key)[1]
    try:
        print("Found file on s3: " + filename)
        BUCKET.download_file(obj.key, filename)
        if not DEBUG:
            upload_to_receiver(filename)
    except error:
        print("Bad Key Found - Skipping: " + error)


def upload_to_receiver(filename):
    data = {'file': (filename, open(filename, 'rb'))}
    request = requests.post(POST_URL, files=data, proxies=POST_PROXIES)
    if request.status_code == requests.codes.ok:
        rename_s3_path(filename)
    else:
        print("Error uploading file to POST_URL: " + POST_URL +
              "Error code: " + request.status_code)
        raise SystemExit(2)


def rename_s3_path(filename):
    try:
        print("Renaming file in s3")
        year = datetime.datetime.now().strftime("%y")
        month = datetime.datetime.now().strftime("%m")
        day = datetime.datetime.now().strftime("%d")
        hour = datetime.datetime.now().strftime("%H")
        BUCKET.upload_file(filename, S3_PROCESSED_DATA_FOLDER + '/' + year +
                           '/' + month + '/' + day + '/' + hour + '/' +
                           filename)
        BUCKET.delete_object(Bucket=S3_BUCKET_NAME, Key=S3_IN_DATA_FOLDER +
                             '/' + filename)
    except:
        raise SystemExit(1)


def remove_local_file(filename):
    try:
        os.remove(filename)
    except error:
        print("Error removing local file: " + filename + " " + error)


S3 = boto3.resource('s3',
                    region_name=S3_REGION,
                    AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY,
                    verify=S3_SSL_VERIFY,
                    S3_URL=S3_URL)
BUCKET = S3.Bucket(S3_BUCKET_NAME)
try:
    S3.meta.client.head_bucket(Bucket=S3_BUCKET_NAME)
except botocore.exceptions.ClientError as error:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    ERROR_CODE = int(error.response['Error']['Code'])
    if ERROR_CODE == 404:
        print("s3 Bucket: " + S3_BUCKET_NAME + " does not exist at S3_URL: " +
              S3_URL)
        raise SystemExit(2)


while True:
    download_file_from_s3()
    time.sleep(SLEEP_TIME)

