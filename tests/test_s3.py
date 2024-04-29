import random
import unittest

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import regions
from simple_aws_wrapper.const.regions import Region
from simple_aws_wrapper.services.s3 import S3


class TestS3(unittest.TestCase):
    def setUp(self) -> None:
        AWSConfig().set_region(Region(regions.EU_WEST_1)).set_endpoint_url(
            "http://localhost:4566"
        ).set_aws_secret_access_key("test").set_aws_access_key_id(
            "test"
        ).set_aws_session_token(
            "test"
        )
        self.s3 = S3()
        self.bucket_name = f"test-bucket-{random.randint(0, 1000)}"
        self.test_string: str = "Hello World!"
        self.object_key = "test.txt"
        self.object_key_for_listing = "test2.txt"
        self.destination_bucket_name = f"test-bucket-{random.randint(0, 1000)}"
        self.destination_object_key = "test-destination.txt"

    def test_put_object(self):
        self.s3.create_bucket(self.bucket_name)
        upload_status: bool = self.s3.put_object(
            self.test_string, self.bucket_name, self.object_key
        )
        self.s3.delete_object(self.bucket_name, self.object_key)
        self.s3.delete_bucket(self.bucket_name)
        self.assertTrue(upload_status)

    def test_create_bucket(self):
        self.assertTrue(self.s3.create_bucket(self.bucket_name))
        self.s3.delete_bucket(self.bucket_name)

    def test_delete_bucket(self):
        self.s3.create_bucket(self.bucket_name)
        self.assertTrue(self.s3.delete_bucket(self.bucket_name))

    def test_delete_object(self):
        self.s3.create_bucket(self.bucket_name)
        self.s3.put_object(self.test_string, self.bucket_name, self.object_key)
        self.assertTrue(self.s3.delete_object(self.bucket_name, self.object_key))
        self.s3.delete_bucket(self.bucket_name)

    def test_get_str_file_content(self):
        self.s3.create_bucket(self.bucket_name)
        self.s3.put_object(
            self.test_string, bucket_name=self.bucket_name, object_key=self.object_key
        )
        file_content: str = self.s3.get_str_file_content(
            self.bucket_name, self.object_key
        )
        self.assertEqual(file_content, self.test_string)
        self.s3.delete_object(self.bucket_name, self.object_key)
        self.s3.delete_bucket(self.bucket_name)

    def test_copy_object(self):
        self.s3.create_bucket(self.bucket_name)
        self.s3.create_bucket(self.destination_bucket_name)
        self.s3.put_object(self.test_string, self.bucket_name, self.object_key)
        self.s3.copy_object(
            self.bucket_name,
            self.object_key,
            self.destination_object_key,
            self.destination_bucket_name,
        )
        self.assertEqual(
            self.s3.get_str_file_content(
                self.destination_bucket_name, self.destination_object_key
            ),
            self.test_string,
        )
        self.s3.delete_object(self.bucket_name, self.object_key)
        self.s3.delete_object(self.destination_bucket_name, self.destination_object_key)
        self.s3.delete_bucket(self.bucket_name)
        self.s3.delete_bucket(self.destination_bucket_name)

    def test_get_file_content(self):
        self.s3.create_bucket(self.bucket_name)
        self.s3.put_object(
            self.test_string, bucket_name=self.bucket_name, object_key=self.object_key
        )
        file_content: bytes = self.s3.get_file_content(
            self.bucket_name, self.object_key
        )
        self.assertEqual(file_content, self.test_string.encode())
        self.s3.delete_object(self.bucket_name, self.object_key)
        self.s3.delete_bucket(self.bucket_name)

    def test_bucket_exists(self):
        self.s3.create_bucket(self.bucket_name)
        self.assertTrue(self.s3.bucket_exists(self.bucket_name))
        self.s3.delete_bucket(self.bucket_name)
        self.assertFalse(self.s3.bucket_exists(self.bucket_name))

    def test_list_objects(self):
        self.s3.create_bucket(self.bucket_name)
        self.s3.put_object(self.test_string, bucket_name=self.bucket_name, object_key=self.object_key)
        self.s3.put_object(self.test_string, bucket_name=self.bucket_name, object_key=self.object_key_for_listing)
        object_key_list: list[str] = self.s3.list_object_keys(self.bucket_name)
        self.assertTrue(self.object_key in object_key_list and self.object_key_for_listing in object_key_list)
