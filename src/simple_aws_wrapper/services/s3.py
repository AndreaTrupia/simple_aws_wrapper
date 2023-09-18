from __future__ import annotations

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import services
from simple_aws_wrapper.exceptions.exceptions import (
    MissingConfigurationException,
    GenericException,
)
from simple_aws_wrapper.resource_manager import ResourceManager


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

    def get_file_content(self, bucket_name: str, object_key: str) -> bytes:
        """
        Funzione per prelevare il contenuto di un file dal bucket
        :param bucket_name: nome del bucket
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :return: contenuto del file codificato in byte
        """
        try:
            file = self.client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = file["Body"].read()
        except Exception as e:
            print(str(e))
            raise GenericException
        return file_content

    def get_str_file_content(self, bucket_name: str, object_key: str) -> str:
        """
        Funzione per prelevare il contenuto di un file dal bucket in formato di stringa
        :param bucket_name: nome del bucket
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :return: contenuto del file in formato di stringa
        """
        try:
            return self.get_file_content(bucket_name, object_key).decode("utf-8")
        except Exception as e:
            print(str(e))
            raise GenericException

    def copy_object(
        self,
        bucket_name: str,
        object_key: str,
        destination_object_key: str,
        destination_bucket_name: str | None = None,
    ) -> bool:
        """
        Funzione per copiare un oggetto dal bucket in un altro
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :param bucket_name: nome del bucket
        :param destination_object_key: objectkey per identificare l'oggetto all'interno del bucket
        :param destination_bucket_name: nome del bucket di destinazione. Se None, la copia avviene all'interno dello
        stesso bucket sorgente
        :return: bool True se l'upload Ã© andato OK, False altrimenti
        """
        try:
            if not destination_bucket_name:
                destination_bucket_name = bucket_name
            self.client.copy_object(
                Bucket=destination_bucket_name,
                Key=destination_object_key,
                CopySource={"Bucket": bucket_name, "Key": object_key},
            )
            return True
        except Exception as e:
            print(str(e))
            raise GenericException

    def delete_object(self, bucket_name: str, object_key: str) -> bool:
        """
        Funzione per cancellare un oggetto dal bucket
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :param bucket_name: nome del bucket
        :return: bool True se l'upload è andato OK, False altrimenti
        """
        try:
            self.client.delete_object(Bucket=bucket_name, Key=object_key)
            return True
        except Exception as e:
            print(str(e))
            raise GenericException

    def move_object(
        self,
        bucket_name: str,
        object_key: str,
        destination_object_key: str,
        destination_bucket_name: str | None = None,
    ) -> bool:
        """
        Funzione per spostare un oggetto da un bucket a un altro
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :param bucket_name: nome del bucket
        :param destination_object_key: objectkey per identificare l'oggetto all'interno del bucket
        :param destination_bucket_name: nome del bucket di destinazione. Se None, la copia avviene all'interno dello
        stesso bucket sorgente
        :return: bool True se l'upload è andato OK, False altrimenti
        """
        if not destination_bucket_name:
            destination_bucket_name = bucket_name
        try:
            self.copy_object(
                bucket_name, object_key, destination_object_key, destination_bucket_name
            )
            self.delete_object(bucket_name, object_key)
            return True
        except Exception as e:
            print(str(e))
            raise GenericException
