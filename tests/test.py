import random
import unittest

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import regions
from simple_aws_wrapper.const.regions import Region
from simple_aws_wrapper.services.s3 import S3


class TestS3(unittest.TestCase):
    def test_s3_put_object(self):
        AWSConfig().set_region(Region(regions.EU_WEST_1)).set_endpoint_url(
            "http://localhost:4566"
        )
        s3 = S3()
        bucket_name = f"test-bucket-{random.randint(0, 1000)}"
        object_key = "test.txt"
        s3.create_bucket(bucket_name)
        upload_status: bool = s3.put_object(b"Hello World!", bucket_name, object_key)
        s3.delete_object(bucket_name, object_key)
        s3.delete_bucket(bucket_name)
        self.assertTrue(upload_status)



