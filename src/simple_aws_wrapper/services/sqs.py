from __future__ import annotations

import json
import traceback

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import services
from simple_aws_wrapper.exceptions.exceptions import (
    MissingConfigurationException,
    GenericException,
)
from simple_aws_wrapper.resource_manager import ResourceManager


class SQS:
    """
    Classe per la gestione di SQS su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.client = ResourceManager.get_client(services.SQS, **AWSConfig().to_dict())

    @staticmethod
    def create_message(**kwargs) -> dict:
        """
        Funzione per creare un dizionario a partire dai kwargs.
        Esempio di utilizzo:
            message:dict = create_message(parametro_a='a', parametro_b=3)
        Produce un dizionario come segue:
            {"parametro_a": "a", "parametro_b": 3}
        :param kwargs: coppie chiave valore con cui popolare il dizionario
        :return: dict
        """
        output_dict: dict = {}
        for k in kwargs.keys():
            output_dict[k] = kwargs[k]
        return output_dict

    def send_json_message(self, queue_name: str, message_body: dict) -> bool:
        """
        Funzione per inviare un messaggio json (dict) verso una coda
        :param queue_name: nome della coda
        :param message_body: corpo del messaggio
        :return: bool
        """
        try:
            queue_url = self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
            self.client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body),
            )
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def send_message(self, queue_name: str, message_body: str | dict) -> bool:
        """
        Funzione per inviare un messaggio in una coda. Il messaggio può essere un dizionario o una stringa. Ambo i casi
        viene trattato come una stringa
        :param queue_name: nome della coda
        :param message_body: corpo del messaggio
        :return: None
        """
        if isinstance(message_body, dict):
            message_body = str(message_body)
        try:
            queue_url = self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
            self.client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
            )
            return True
        except Exception:
            raise GenericException(traceback.format_exc())
