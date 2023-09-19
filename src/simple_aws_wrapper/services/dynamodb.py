from __future__ import annotations

import decimal
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

    def __get_table_resource(self, table_name: str):
        """
        Funzione per effettuare la get di una risorsa di tipo tabella di DynamoDB
        :param table_name: nome tabella
        :return: dynamodb.Table
        """
        try:
            return self.client.Table(table_name)
        except Exception as e:
            print(str(e))
            raise GenericException

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
        except Exception as e:
            print(str(e))
            raise GenericException

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
        except Exception as e:
            print(str(e))
            raise GenericException

    def scan_table(self, table_name: str) -> List[dict]:
        """
        Funzione per prelevare tutti gli elementi all'interno di una tabella
        :param table_name: nome tabella
        :return: lista in cui ogni elemento è un dizionario le cui chiavi sono i campi del db e i valori sono i
        rispettivi valori
        """
        try:
            return self.__get_table_resource(table_name).scan()
        except Exception as e:
            print(str(e))
            raise GenericException

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
        except Exception as e:
            print(str(e))
            raise GenericException

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
        except Exception as e:
            print(str(e))
            raise GenericException

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
        except Exception as e:
            print(str(e))
            raise GenericException

    def key_exists(self, table_name: str, key: dict) -> bool:
        """
        Funzione per verificare se un elemento esiste all'interno di una tabella
        :param table_name: nome tabella
        :param key: chiave del record da verificare
        :return: Booleano che indica se l'elemento esiste o meno
        """
        try:
            return self.get_record(table_name, key)["Item"] is not None
        except Exception as e:
            print(str(e))
            raise GenericException

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
        except Exception as e:
            print(str(e))
            raise GenericException

    def load(self, table_name: str):
        """
        Funzione per caricare la tabella
        :param table_name: nome tabella
        :return:
        """
        try:
            return self.__get_table_resource(table_name).load()
        except Exception as e:
            print(str(e))
            raise GenericException
