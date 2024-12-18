import random
import unittest

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import regions
from simple_aws_wrapper.const.regions import Region
from simple_aws_wrapper.services.dynamodb import DynamoDB


class TestDynamoDB(unittest.TestCase):
    def setUp(self) -> None:
        AWSConfig().set_region(Region(regions.EU_WEST_1)).set_endpoint_url(
            "http://localhost:4566"
        ).set_aws_secret_access_key("test").set_aws_access_key_id(
            "test"
        ).set_aws_session_token(
            "test"
        )
        self.dynamodb = DynamoDB()
        self.table_name = f"test-table-{random.randint(0, 1000)}"
        self.key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]
        self.attribute_definitions = [{"AttributeName": "id", "AttributeType": "S"}]
        self.provisioned_throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}

        # self.object_key = "test.txt"
        # self.destination_bucket_name = f"test-bucket-{random.randint(0, 1000)}"
        # self.destination_object_key = "test-destination.txt"

    def test_create_table(self):
        self.assertTrue(
            self.dynamodb.create_table(
                self.table_name,
                self.key_schema,
                self.attribute_definitions,
                self.provisioned_throughput,
            )
        )
        self.assertTrue(self.dynamodb.delete_table(self.table_name))

    def test_put_item(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.assertTrue(self.dynamodb.put_item(self.table_name, {"id": "1"}))
        self.assertTrue(self.dynamodb.delete_item(self.table_name, {"id": "1"}))
        self.dynamodb.delete_table(self.table_name)

    def test_get_item(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.assertTrue(self.dynamodb.put_item(self.table_name, {"id": "1"}))
        self.assertEqual(
            self.dynamodb.get_item(self.table_name, {"id": "1"}), {"id": "1"}
        )
        self.dynamodb.delete_table(self.table_name)

    def test_get_record(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1"})
        self.assertTrue(
            "Item" in self.dynamodb.get_record(self.table_name, {"id": "1"})
        )
        self.assertTrue(
            "ResponseMetadata" in self.dynamodb.get_record(self.table_name, {"id": "1"})
        )
        self.assertEqual(
            self.dynamodb.get_record(self.table_name, {"id": "1"})["Item"]["id"], "1"
        )
        self.dynamodb.delete_table(self.table_name)

    def test_scan_table(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1"})
        self.dynamodb.put_item(self.table_name, {"id": "2"})
        self.assertTrue("Items" in self.dynamodb.scan_table(self.table_name))
        self.assertEqual(self.dynamodb.scan_table(self.table_name)["Count"], 2)
        self.assertEqual(
            self.dynamodb.scan_table(self.table_name)["Items"][0]["id"], "1"
        )
        self.dynamodb.delete_table(self.table_name)

    def test_update_item(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1", "test_value": "1"})
        update_expression = "SET test_value = :test_value"
        expression_attribute_values = {":test_value": "2"}
        self.assertTrue(
            self.dynamodb.update_item(
                self.table_name,
                {"id": "1"},
                update_expression,
                expression_attribute_values,
            )
        )
        self.assertEqual(
            self.dynamodb.get_item(self.table_name, {"id": "1"}),
            {"id": "1", "test_value": "2"},
        )
        self.dynamodb.delete_table(self.table_name)

    def test_get_item_value(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1", "test_value": "1"})
        self.assertEqual(
            self.dynamodb.get_item_value(self.table_name, {"id": "1"}, "test_value"),
            "1",
        )
        self.dynamodb.delete_table(self.table_name)

    def test_delete_table(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.assertTrue(self.dynamodb.delete_table(self.table_name))

    def test_key_exists(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1"})
        self.assertTrue(self.dynamodb.key_exists(self.table_name, {"id": "1"}))
        self.assertFalse(self.dynamodb.key_exists(self.table_name, {"id": "2"}))
        self.dynamodb.delete_table(self.table_name)

    def test_delete_item(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1"})
        self.assertTrue(self.dynamodb.delete_item(self.table_name, {"id": "1"}))
        self.assertFalse(self.dynamodb.key_exists(self.table_name, {"id": "1"}))
        self.dynamodb.delete_table(self.table_name)

    def test_scan_filter_elements(self):
        self.dynamodb.create_table(
            self.table_name,
            self.key_schema,
            self.attribute_definitions,
            self.provisioned_throughput,
        )
        self.dynamodb.put_item(self.table_name, {"id": "1", "test_value": "1"})
        self.dynamodb.put_item(self.table_name, {"id": "2", "test_value": "2"})
        self.dynamodb.put_item(self.table_name, {"id": "3", "test_value": "2"})
        self.dynamodb.put_item(self.table_name, {"id": "4", "test_value_bool": True})
        self.dynamodb.put_item(self.table_name, {"id": "5", "test_value_bool": False})
        self.assertEqual(
            [{"id": "4", "test_value_bool": True}],
            self.dynamodb.scan_filter_elements(
                self.table_name,
                "test_value_bool",
                True,
                "BOOL",
            ),
        )
        self.assertEqual(
            [{"id": "1", "test_value": "1"}],
            self.dynamodb.scan_filter_elements(
                self.table_name,
                "test_value",
                "1",
                "S",
            ),
        )
        self.dynamodb.delete_table(self.table_name)
