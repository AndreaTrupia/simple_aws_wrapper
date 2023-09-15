from __future__ import annotations

from typing import List

from src.simple_aws_wrapper.config import AWSConfig
from src.simple_aws_wrapper.const import services
from src.simple_aws_wrapper.exceptions.exceptions import (
    GenericException,
    MissingConfigurationException,
)


class DynamoDB:
    """
    Classe per la gestione di DynamoDB su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.region_name = AWSConfig().get_region_name()
        self.endpoint_url = AWSConfig().get_endpoint_url()

    def __get_table_resource(self, table_name: str):
        """
        Funzione per effettuare la get di una risorsa di tipo tabella di DynamoDB
        :param table_name: nome tabella
        :return: dynamodb.Table
        """
        try:
            if self.endpoint_url and self.endpoint_url != "":
                dynamodb_table = AWSConfig.get_resource(
                    services.DYNAMO_DB, self.region_name, self.endpoint_url
                ).Table(table_name)
            else:
                dynamodb_table = AWSConfig.get_resource(
                    services.DYNAMO_DB, self.region_name
                ).Table(table_name)
            return dynamodb_table
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
        if self.endpoint_url and self.endpoint_url != "":
            dynamodb_table = AWSConfig.get_resource(
                services.DYNAMO_DB, self.region_name, self.endpoint_url
            ).Table(table_name)
        else:
            dynamodb_table = AWSConfig.get_resource(
                services.DYNAMO_DB, self.region_name
            ).Table(table_name)
        try:
            dynamodb_table.put_item(Item=item)
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
