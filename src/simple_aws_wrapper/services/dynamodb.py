from __future__ import annotations

from typing import List

from src.simple_aws_wrapper.config import AWSConfig
from src.simple_aws_wrapper.const import services
from src.simple_aws_wrapper.exceptions.exceptions import (
    GenericException,
    MissingConfigurationException,
)
from src.simple_aws_wrapper.resource_manager import ResourceManager


class DynamoDB:
    """
    Classe per la gestione di DynamoDB su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.region_name = AWSConfig().get_region_name()
        self.endpoint_url = AWSConfig().get_endpoint_url()
        if self.endpoint_url and self.endpoint_url != "":
            self.client = ResourceManager.get_resource(services.S3, self.region_name, self.endpoint_url)
        else:
            self.client = ResourceManager.get_resource(services.S3, self.region_name)

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
