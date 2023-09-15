from __future__ import annotations

from config import AWSConfig
from const import services
from exceptions.exceptions import MissingConfigurationException, GenericException
from resource_manager import ResourceManager


class S3:
    """
    Classe per la gestione di bucket S3 su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.region_name = AWSConfig().get_region_name()
        self.endpoint_url = AWSConfig().get_endpoint_url()
        if self.endpoint_url and self.endpoint_url != "":
            self.client = ResourceManager.get_client(
                services.S3, self.region_name, self.endpoint_url
            )
        else:
            self.client = ResourceManager.get_client(services.S3, self.region_name)

    def put_object(self, body: bytes | str, bucket_name: str, object_key: str) -> bool:
        """
        Funzione per inserire un oggetto all'interno di un bucket
        :param body: contenuto del file codificato in byte
        :param bucket_name: nome del buket su cui effettuare l'upload
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :return: bool True se l'upload è andato OK, False altrimenti
        """
        if isinstance(body, str):
            body = body.encode("utf-8")
        try:
            self.client.put_object(Body=body, Bucket=bucket_name, Key=object_key)
            return True
        except Exception as e:
            print(str(e))
            raise GenericException
