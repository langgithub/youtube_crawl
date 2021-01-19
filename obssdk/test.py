import os
import time

from obs import ObsOperator, MultipartUploadFileRequest
from obs.util import *
from io import BytesIO

obs_access_key = 'QTU2QjkyRjFFRDhGNDJDMzlBRkFDQUUyMTU5Q0Y5REM'
obs_secret_key = 'QzFBODBEQTFDNEVENEU1NDlDQkEwNjMwQzk2MzU2MkM'

# 深圳地址
obs_host = "obs-cn-shenzhen.yun.pingan.com"
bucket_name = "spider"

key = "qh1lGCf0AOGUOWDMQ9Pi8GTCIGvxIDPObGv5wCM7"

object_key = "test12345xxx99.pdf"

upload_content = "ttt12345"
# local_file_upload_path = "tests\\temp\\test_upload.txt"
local_file_upload_path = "tests/temp/test_upload.txt"
# local_file_download_path = "tests\\temp\\test_download.txt"
local_file_download_path = "tests/temp/test_download.txt"
# multipart_upload_file_path = "tests\\temp\\apache-tomcat-7.0.90.tar.gz"
multipart_upload_file_path = "tests/temp/apache-tomcat-7.0.90.tar.gz"
# multipart_download_file_path = "tests\\temp\\download-apache-tomcat-7.0.90.tar.gz"
multipart_download_file_path = "tests/temp/download-apache-tomcat-7.0.90.tar.gz"
user_metadata = {'width': '300', 'height': '300'}


def test_111_upload_multipart():
    obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)

    multipart_request = MultipartUploadFileRequest()
    multipart_request.set_bucket_name(bucket_name)
    multipart_request.set_object_key(object_key)
    multipart_request.set_upload_file_path(multipart_upload_file_path)
    multipart_request.set_upload_notifier(notify_upload)

    obs.put_object_multipart(multipart_request)


def test_112_download_multipart():
    obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
    s3_object = obs.get_object(bucket_name, object_key)
    with open(multipart_download_file_path, "wb") as to_file:
        s3_object.to_file(to_file)

    # 比较文件的大小是否相等
    upload_file_size = os.path.getsize(multipart_upload_file_path)
    download_file_size = os.path.getsize(multipart_download_file_path)
    assert upload_file_size == download_file_size

    # 比较两个文件的摘要是否相等
    upload_file_digest = md5_hex_digest_from_file(multipart_upload_file_path)
    download_file_digest = md5_hex_digest_from_file(multipart_download_file_path)
    assert upload_file_digest == download_file_digest


def notify_upload(upload_id, state, total_parts, finish_parts):
    print("%s %s %s of %s" % (upload_id, state, finish_parts, total_parts))

def put_stream():
    import requests
    import urllib
    # input_stream = urllib.request.urlopen("http://www.iachina.cn/IC/tkk/03/785acf24-3588-4ef5-91b1-8fe7bf964a4d_TERMS.PDF")
    import tempfile
    input_stream = requests.get("http://www.iachina.cn/IC/tkk/03/785acf24-3588-4ef5-91b1-8fe7bf964a4d_TERMS.PDF", stream=True)
    # print(input_stream.text)
    from io import BytesIO,BufferedReader
    file_like = BufferedReader(BytesIO(input_stream.content))
    print(type(file_like))
    obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
    # b= BytesIO(input_stream.content)
    ret = obs.put_object(bucket_name, object_key, file_like)
    print(ret)

def run():
    # t0 = time.time()
    # test_111_upload_multipart()
    # print(time.time() - t0)
    # t1 = time.time()
    # test_112_download_multipart()
    # print(time.time() - t1)
    put_stream()


if __name__ == "__main__":
    run()
