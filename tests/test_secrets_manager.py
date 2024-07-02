import random
import string
import unittest

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import regions
from simple_aws_wrapper.const.regions import Region
from simple_aws_wrapper.services.secrets_manager import SecretsManager


class TestSecretsManager(unittest.TestCase):
    @staticmethod
    def generate_1mb_string() -> str:
        size_in_bytes = 1 * 1024 * 1024
        characters = string.ascii_letters + string.digits
        result_string = "".join(random.choices(characters, k=size_in_bytes))
        return result_string

    def setUp(self) -> None:
        AWSConfig().set_region(Region(regions.EU_WEST_1)).set_endpoint_url(
            "http://localhost:4566"
        ).set_aws_secret_access_key("test").set_aws_access_key_id(
            "test"
        ).set_aws_session_token(
            "test"
        )
        self.secrets_manager = SecretsManager()
        self.secret_name = f"test-secret-{random.randint(0, 1000)}"
        self.secret_string = self.generate_1mb_string()
        self.binary_secret_name = f"test-binary-secret-{random.randint(0, 1000)}"
        with open("resources/test_binary_file.docx", "rb") as f:
            self.secret_bytes = f.read()

    def test_create_secret(self):
        self.secrets_manager.create_secret(
            name=self.secret_name,
            secret_string=self.secret_string,
        )
        secret_id = self.secrets_manager.get_secret_id_by_name(self.secret_name)
        secret_value = self.secrets_manager.get_secret_value(secret_id=secret_id)
        self.assertEqual(secret_value, self.secret_string)
        self.assertTrue(
            self.secrets_manager.get_secret_name_by_id(secret_id)
            in [x["Name"] for x in self.secrets_manager.list_secrets()]
        )
        self.assertTrue(self.secrets_manager.delete_secret(secret_id=secret_id))

    def test_create_binary_secret(self):
        self.secrets_manager.create_binary_secret(
            name=self.binary_secret_name,
            secret_binary=self.secret_bytes,
        )
        secret_id = self.secrets_manager.get_secret_id_by_name(self.binary_secret_name)
        secret_value = self.secrets_manager.get_binary_secret_value(secret_id=secret_id)
        self.assertEqual(secret_value, self.secret_bytes)
        self.assertTrue(
            self.secrets_manager.get_secret_name_by_id(secret_id)
            in [x["Name"] for x in self.secrets_manager.list_secrets()]
        )
        self.assertTrue(self.secrets_manager.delete_secret(secret_id=secret_id))
