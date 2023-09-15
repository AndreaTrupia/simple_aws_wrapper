from __future__ import annotations

from config import AWSConfig
from const import services
from exceptions.exceptions import MissingConfigurationException, GenericException
from resource_manager import ResourceManager


class SQS:
    """
    Classe per la gestione di SQS su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.region_name = AWSConfig().get_region_name()
        self.endpoint_url = AWSConfig().get_endpoint_url()
        if self.endpoint_url and self.endpoint_url != "":
            self.client = ResourceManager.get_client(
                services.SQS, self.region_name, self.endpoint_url
            )
        else:
            self.client = ResourceManager.get_client(services.SQS, self.region_name)

    def create_message(self, **kwargs) -> dict:
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
        except Exception as e:
            print(str(e))
            raise GenericException
