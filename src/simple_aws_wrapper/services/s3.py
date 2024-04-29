from __future__ import annotations

import sys
import traceback

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import services, regions
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
        self.client = ResourceManager.get_global_client(
            services.S3, **AWSConfig().to_dict()
        )
        self.region_name = AWSConfig().get_region_name()

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
        except Exception:
            raise GenericException(traceback.format_exc())

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
        except Exception:
            raise GenericException(traceback.format_exc())
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
        except Exception:
            raise GenericException(traceback.format_exc())

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
        except Exception:
            raise GenericException(traceback.format_exc())

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
        except Exception:
            raise GenericException(traceback.format_exc())

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
        except Exception:
            raise GenericException(traceback.format_exc())

    def create_bucket(self, bucket_name: str, **kwargs):
        """
        Funzione per creare un bucket
        :param bucket_name: nome del bucket
        :param kwargs: argomenti variabili
        :return: True se la creazione è andata bene
        """
        try:
            if self.region_name != regions.US_EAST_1:
                kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.region_name
                }
            self.client.create_bucket(Bucket=bucket_name, **kwargs)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def delete_bucket(self, bucket_name: str):
        """
        Funzione per eliminare un bucket
        :param bucket_name: nome del bucket
        :return: True se la creazione Ã© andata bene
        """
        try:
            self.client.delete_bucket(Bucket=bucket_name)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except Exception:
            return False

    def list_object_keys(self, bucket_name: str) -> list[str]:
        try:
            result = self.client.list_objects(Bucket=bucket_name, Delimiter='/')
            output_list: list[str] = []
            for object in result["Contents"]:
                output_list.append(object["Key"])
            return output_list
        except Exception:
            raise GenericException(traceback.format_exc())
