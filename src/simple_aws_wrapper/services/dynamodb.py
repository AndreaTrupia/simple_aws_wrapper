from __future__ import annotations

from typing import List

from src.simple_aws_wrapper.ResourceManager import ResourceManager
from src.simple_aws_wrapper.enums import services
from src.simple_aws_wrapper.exceptions.generic_exception import GenericException


class DynamoDB:
    """
    Classe per la gestione di DynamoDB su AWS
    """

    def __get_table_resource(self, table_name: str):
        """
        Funzione per effettuare la get di una risorsa di tipo tabella di DynamoDB
        :param table_name: nome tabella
        :return: dynamodb.Table
        """
        try:
            if self.endpoint_url and self.endpoint_url != "":
                dynamodb_table = ResourceManager.get_resource(
                    services.DYNAMO_DB, self.region_name, self.endpoint_url
                ).Table(table_name)
            else:
                dynamodb_table = ResourceManager.get_resource(services.DYNAMO_DB, self.region_name).Table(table_name)
            return dynamodb_table
        except Exception as e:
            print(str(e))
            raise GenericException

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def put_item(self, table_name: str, item: dict) -> bool:
        """
        Funzione per inserire una enry all'interno di una tabella
        :param table_name: nome della tabella in cui effettuare l'inserimento
        :param item: entry da inserire sotto forma di dizionario chiave-valore
        :return: None
        """
        if self.endpoint_url and self.endpoint_url != "":
            dynamodb_table = ResourceManager.get_resource(
                services.DYNAMO_DB, self.region_name, self.endpoint_url
            ).Table(table_name)
        else:
            dynamodb_table = ResourceManager.get_resource(services.DYNAMO_DB, self.region_name).Table(table_name)
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
