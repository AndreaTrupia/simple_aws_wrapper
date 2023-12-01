from unittest import TestCase

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import regions
from simple_aws_wrapper.const.regions import Region
from simple_aws_wrapper.services.parameter_store import ParameterStore


class TestParameterStore(TestCase):
    def setUp(self) -> None:
        AWSConfig().set_region(Region(regions.EU_WEST_1)).set_endpoint_url(
            "http://localhost:4566"
        ).set_aws_secret_access_key("test").set_aws_access_key_id(
            "test"
        ).set_aws_session_token(
            "test"
        )
        self.parameter_store = ParameterStore()

    def test_get_parameters_values_from_list(self):
        N: int = 23
        key_list = [f"/{str(i)}/{str(i)}" for i in range(N)]
        values_list = [str(i) for i in range(N)]
        expected_output = dict(zip(key_list, values_list))
        for i in range(N):
            self.parameter_store.create_parameter(
                f"/{str(i)}/{str(i)}", str(i), ParameterStore.Type.STRING
            )
        self.assertEqual(
            expected_output,
            self.parameter_store.get_parameters_values_from_list(key_list),
        )
        for i in range(N):
            self.assertTrue(self.parameter_store.delete_parameter(f"/{str(i)}/{str(i)}"))

    def test_create_and_delete_parameter(self):
        self.assertTrue(
            self.parameter_store.create_parameter(
                "test", "test", ParameterStore.Type.STRING
            )
        )
        self.assertTrue(self.parameter_store.delete_parameter("test"))
