from obs import ObsOperator, ObjectMetadata, MultipartUploadFileRequest
from obs.util import *
import unittest
from urllib.request import urlopen
import os

# obs_access_key = "gQ5QlDwOeQZhaxy3K0FYNZycY9n2t9XnTPcHMDDDLq-G8yHXcRVD8g6XVbSkPdnFat6NXcn-8TPhLEOwc-JhqQ"
obs_access_key = 'QTU2QjkyRjFFRDhGNDJDMzlBRkFDQUUyMTU5Q0Y5REM'
# obs_secret_key = "4mqgizIO401B4_kSh3ZB0EI4e0AmRwyjmvbXqybSA8tgz-DNaAXf8pOiLeKm-p2Farc1qGXA9XWxoABKqtf7TQ"
obs_secret_key = 'QzFBODBEQTFDNEVENEU1NDlDQkEwNjMwQzk2MzU2MkM'

object_key_directory = "test123ddd/高分dsf"
object_key = "test12345xxx99"

# upload | download | all
flag = 'all'

# 本地地址
# obs_host = "localhost:8080"
# bucket_name = "yanzheng-test-hd03"

# 深圳地址
obs_host = "obs-cn-shenzhen.yun.pingan.com"
bucket_name = "spider"

# 香港地址
# obs_host = "obs-cn-hongkong-test.paic.com.cn:8080"
# bucket_name = "testhk7240"

# 直接连接ceph层地址
# obs_host = "10.25.84.80"
# bucket_name = "yanzheng-test-hd03"
# obs_access_key = "5NE0EMKT1XAIF1WRUSDB"
# obs_secret_key = "qh1lGCf0AOGUOWDMQ9Pi8GTCIGvxIDPObGv5wCM7"

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


class TestObsOperator(unittest.TestCase):

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_100_upload_content(self):
        object_metadata = ObjectMetadata()
        object_metadata.add_metadata(HTTP_X_AMZ_DATE, get_gmt_time())
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        ret = obs.put_object(bucket_name, object_key, upload_content, object_metadata)
        self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'upload', u'测试上传，跳过下载测试')
    def test_101_download_content(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        s3_object = obs.get_object(bucket_name, object_key)
        self.assertEqual(upload_content, s3_object.get_object_stream().read().decode())

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_102_upload_local_file(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        with open(local_file_upload_path, "rb") as from_file:
            ret = obs.put_object(bucket_name, object_key, from_file)
            self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_103_upload_local_file(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        with open(local_file_upload_path, "rb") as from_file:
            ret = obs.put_object_from_file(bucket_name, object_key, from_file)
            self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_104_upload_local_file(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        ret = obs.put_object_from_file(bucket_name, object_key, local_file_upload_path)
        self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'upload', u'测试上传，跳过下载测试')
    def test_105_download_local_file(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        s3_object = obs.get_object(bucket_name, object_key)
        with open(local_file_download_path, "wb") as to_file:
            s3_object.to_file(to_file)

        with open(local_file_upload_path, "rb") as upload_file:
            expect_content = upload_file.read().decode()
            with open(local_file_download_path, "rb") as download_file:
                actual_content = download_file.read().decode()
                self.assertEqual(expect_content, actual_content)

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_106_upload_content_with_user_metadata(self):
        object_metadata = ObjectMetadata()
        for k, v in user_metadata.items():
            object_metadata.add_user_metadata(k, v)

        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        ret = obs.put_object(bucket_name, object_key, upload_content, object_metadata)
        self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'upload', u'测试上传，跳过下载测试')
    def test_107_download_content_with_user_metadata(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        s3_object = obs.get_object(bucket_name, object_key)
        self.assertEqual(upload_content, s3_object.get_object_stream().read().decode())
        self.assertEqual(user_metadata, s3_object.get_object_metadata().get_user_metadata())

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_108_upload_local_file_with_user_metadata(self):
        object_metadata = ObjectMetadata()
        for k, v in user_metadata.items():
            object_metadata.add_user_metadata(k, v)
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        with open(local_file_upload_path, "rb") as from_file:
            ret = obs.put_object_from_file(bucket_name, object_key, from_file, object_metadata)
            self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'upload', u'测试上传，跳过下载测试')
    def test_109_download_local_file_with_user_metadata(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        s3_object = obs.get_object(bucket_name, object_key)
        with open(local_file_download_path, "wb") as to_file:
            s3_object.to_file(to_file)

        with open(local_file_upload_path, "rb") as upload_file:
            expect_content = upload_file.read().decode()
            with open(local_file_download_path, "rb") as download_file:
                actual_content = download_file.read().decode()
                self.assertEqual(expect_content, actual_content)
        self.assertEqual(user_metadata, s3_object.get_object_metadata().get_user_metadata())

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_110_upload_network_stream(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        input_stream = urlopen("http://alm.paic.com.cn/project/2008/board/")
        ret = obs.put_object(bucket_name, object_key, input_stream)
        self.assertTrue(ret.get_e_tag())

    @unittest.skipIf(flag == 'download', u'测试下载，跳过上传测试')
    def test_111_upload_multipart(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)

        multipart_request = MultipartUploadFileRequest()
        multipart_request.set_bucket_name(bucket_name)
        multipart_request.set_object_key(object_key)
        multipart_request.set_upload_file_path(multipart_upload_file_path)
        multipart_request.set_upload_notifier(notify_upload)

        obs.put_object_multipart(multipart_request)

    @unittest.skipIf(flag == 'upload', u'测试上传，跳过下载测试')
    def test_112_download_multipart(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        s3_object = obs.get_object(bucket_name, object_key)
        with open(multipart_download_file_path, "wb") as to_file:
            s3_object.to_file(to_file)

        # 比较文件的大小是否相等
        upload_file_size = os.path.getsize(multipart_upload_file_path)
        download_file_size = os.path.getsize(multipart_download_file_path)
        self.assertEqual(upload_file_size, download_file_size)

        # 比较两个文件的摘要是否相等
        upload_file_digest = md5_hex_digest_from_file(multipart_upload_file_path)
        download_file_digest = md5_hex_digest_from_file(multipart_download_file_path)
        self.assertEqual(upload_file_digest, download_file_digest)

    def test_113_upload_content_with_directory(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        ret = obs.put_object(bucket_name, object_key_directory, upload_content)
        self.assertTrue(ret.get_e_tag())

    def test_114_download_content_with_directory(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        s3_object = obs.get_object(bucket_name, object_key_directory)
        self.assertEqual(upload_content, s3_object.get_object_stream().read().decode())

    def test_115_upload_local_file_with_directory(self):
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        ret = obs.put_object_from_file(bucket_name, object_key_directory, local_file_upload_path)
        self.assertTrue(ret.get_e_tag())


def notify_upload(upload_id, state, total_parts, finish_parts):
    print("%s %s %s of %s" % (upload_id, state, finish_parts, total_parts))


if __name__ == "__main__":
    unittest.main()
