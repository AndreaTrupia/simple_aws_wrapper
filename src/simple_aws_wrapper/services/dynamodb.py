from __future__ import annotations

import decimal
import traceback
from typing import List

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import services
from simple_aws_wrapper.exceptions.exceptions import (
    MissingConfigurationException,
    GenericException,
)
from simple_aws_wrapper.resource_manager import ResourceManager


class DynamoDB:
    """
    Classe per la gestione di DynamoDB su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.client = ResourceManager.get_resource(
            services.DYNAMO_DB, **AWSConfig().to_dict()
        )
        self.__dynamodb = ResourceManager.get_client(
            services.DYNAMO_DB, **AWSConfig().to_dict()
        )

    def __get_table_resource(self, table_name: str):
        """
        Funzione per effettuare la get di una risorsa di tipo tabella di DynamoDB
        :param table_name: nome tabella
        :return: dynamodb.Table
        """
        try:
            return self.client.Table(table_name)
        except Exception:
            raise GenericException(traceback.format_exc())

    def get_record(self, table_name: str, key: dict):
        """
        Funzione per prelevare un record all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da prelevare
        :return: Dizionario con i valori del record
        """
        try:
            output = self.__get_table_resource(table_name).get_item(Key=key)
            if "Item" not in output:
                return None
            return output
        except Exception:
            raise GenericException(traceback.format_exc())

    def put_item(self, table_name: str, item: dict) -> bool:
        """
        Funzione per inserire una enry all'interno di una tabella
        :param table_name: nome della tabella in cui effettuare l'inserimento
        :param item: entry da inserire sotto forma di dizionario chiave-valore
        :return: None
        """
        try:
            self.__get_table_resource(table_name).put_item(Item=item)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def scan_table(self, table_name: str) -> dict:
        """
        Funzione per prelevare tutti gli elementi all'interno di una tabella
        :param table_name: nome tabella
        :return: lista in cui ogni elemento è un dizionario le cui chiavi sono i campi del db e i valori sono i
        rispettivi valori
        """
        try:
            return self.__get_table_resource(table_name).scan()
        except Exception:
            raise GenericException(traceback.format_exc())

    def update_item(
        self,
        table_name: str,
        key: dict,
        update_expression: str,
        expression_attribute_values: dict,
    ) -> bool:
        """
        Funzione per aggiornare un elemento all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da aggiornare
        :param update_expression: espressione di aggiornamento
        :param expression_attribute_values: dizionario con i valori dei parametri
        :return: Booleano che indica se l'operazione è andata a buon fine o meno
        """
        try:
            self.__get_table_resource(table_name).update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
            )
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def get_item(self, table_name: str, key: dict) -> dict | None:
        """
        Funzione per prelevare un elemento all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da prelevare
        :return: Dizionario con i valori del record
        """
        try:
            return self.get_record(table_name, key)["Item"]
        except TypeError as te:
            print("Record not found")
            return None
        except KeyError as ke:
            print("Record not found")
            return None
        except Exception:
            raise GenericException(traceback.format_exc())

    def get_item_value(
        self, table_name: str, key: dict, attribute_name: str
    ) -> decimal.Decimal | str | bool | None:
        """
        Funzione per prelevare un elemento all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da prelevare
        :param attribute_name: nome dell'attributo da prelevare dal record estratto
        :return: Dizionario con i valori del record
        """
        try:
            return self.get_record(table_name, key)["Item"][attribute_name]
        except TypeError as te:
            print(str(te))
            print("Record not found")
            return None
        except KeyError as ke:
            print(f'Record has no attribute "{attribute_name}"')
            return None
        except Exception:
            raise GenericException(traceback.format_exc())

    def key_exists(self, table_name: str, key: dict) -> bool:
        """
        Funzione per verificare se un elemento esiste all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da verificare
        :return: Booleano che indica se l'elemento esiste o meno
        """
        try:
            return self.get_record(
                table_name, key
            ) is not None and "Item" in self.get_record(table_name, key)
        except Exception:
            raise GenericException(traceback.format_exc())

    def delete_item(self, table_name: str, key: dict) -> bool:
        """
        Funzione per eliminare un elemento all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da eliminare
        :return: Booleano che indica se l'operazione Ã© andata a buon fine o meno
        """
        try:
            self.__get_table_resource(table_name).delete_item(Key=key)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def load(self, table_name: str):
        """
        Funzione per caricare la tabella
        :param table_name: nome tabella
        :return:
        """
        try:
            return self.__get_table_resource(table_name).load()
        except Exception:
            raise GenericException(traceback.format_exc())

    def create_table(
        self,
        table_name: str,
        key_schema: List[dict],
        attribute_definitions: List[dict],
        provisioned_throughput: dict,
        **kwargs,
    ):
        """
        Funzione per creare una tabella
        :param table_name: nome tabella
        :param key_schema: dizionario con i campi della chiave della tabella
        :param attribute_definitions: dizionario con i campi della tabella
        :param provisioned_throughput: dizionario con i parametri di ricerca della tabella
        :param kwargs: argomenti variabili
        :return: bool
        """
        try:
            self.client.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput=provisioned_throughput,
                **kwargs,
            )
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def delete_table(self, table_name: str) -> bool:
        """
        Funzione per cancellare una tabella
        :param table_name: nome tabella
        :return: True se cancellazione va a buon fine
        """
        try:
            self.__get_table_resource(table_name).delete()
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def scan_filter_elements(
        self, table_name: str, column_name: str, value: any, type: str
    ) -> list[dict] | None:
        response = self.__dynamodb.scan(
            TableName=table_name,
            FilterExpression=f"{column_name} = :val",
            ExpressionAttributeValues={":val": {type: value}},
        )
        output_list: list[dict] = []
        items = response.get("Items", [])
        output_list.extend(items)
        while "LastEvaluatedKey" in response:
            response = self.__dynamodb.scan(
                TableName=table_name,
                FilterExpression=f"{column_name} = :val",
                ExpressionAttributeValues={":val": {type: value}},
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items = response.get("Items", [])
            output_list.extend(items)
        for item in output_list:
            for k, v in item.items():
                item[k] = list(v.values())[0]
        return output_list
