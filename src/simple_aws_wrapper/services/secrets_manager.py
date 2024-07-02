from __future__ import annotations

import base64

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import services
from simple_aws_wrapper.exceptions.exceptions import (
    MissingConfigurationException,
    GenericException,
)
from simple_aws_wrapper.resource_manager import ResourceManager

import traceback


class SecretsManager:
    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.client = ResourceManager.get_client(
            services.SECRETS_MANAGER, **AWSConfig().to_dict()
        )
        self.region_name = AWSConfig().get_region_name()

    def create_secret(self, name: str, secret_string: str, **kwargs) -> bool:
        """
        Crea un secret
        :param name: nome del secret
        :param secret_string: stringa del secret
        :param tags: tag per il secret
        :return: dict del secret creato
        """
        try:
            self.client.create_secret(Name=name, SecretString=secret_string, **kwargs)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def create_binary_secret(self, name: str, secret_binary: bytes, **kwargs) -> bool:
        """
        Crea un secret binario
        :param name: nome del secret
        :param secret_binary: bytes del secret
        :param tags: tag per il secret
        :return: dict del secret creato
        """
        try:
            self.client.create_secret(Name=name, SecretBinary=secret_binary, **kwargs)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def get_secret_value(self, secret_id: str, **kwargs) -> dict:
        """
        Recupera un secret
        :param secret_id: id del secret
        :return: dict del secret
        """
        return self.client.get_secret_value(SecretId=secret_id, **kwargs)[
            "SecretString"
        ]

    def get_binary_secret_value(self, secret_id: str, **kwargs):
        """
        Recupera un secret binario
        :param secret_id: id del secret
        :return: bytes del secret
        """
        try:
            return base64.b64decode(self.client.get_secret_value(SecretId=secret_id, **kwargs)[
                "SecretBinary"
            ])
        except Exception:
            raise GenericException(traceback.format_exc())

    def get_secret_id_by_name(self, name: str, **kwargs) -> str:
        """
        Trova l'id del secret by name
        :param name: nome del secret
        :return: id del secret
        """
        try:
            return self.client.get_secret_value(SecretId=name, **kwargs)["ARN"]
        except Exception:
            raise GenericException(traceback.format_exc())

    def delete_secret(self, secret_id: str, **kwargs) -> bool:
        """
        Elimina un secret
        :param secret_id: id del secret
        :return: True se la creazione Ã© andata bene
        """
        try:
            self.client.delete_secret(SecretId=secret_id, **kwargs)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def list_secrets(self, **kwargs) -> list:
        """
        Lista tutti i secrets. Ogni elemento contiene un dizionario con le seguenti chiavi:
            - ARN (Amazon Resource Name) del secret
            - Name (nome) del secret
            - CreatedDate (data di creazione del secret)
            - SecretVersionsToStages (mappa delle versioni del secret a stage)
            - LastChangedDate (data dell'ultima modifica del secret)
            - LastAccessedDate (data dell'ultima accesso al secret)
        :param kwargs: parametri aggiuntivi
        :return: lista dei secrets
        """
        try:
            return self.client.list_secrets(**kwargs)["SecretList"]
        except Exception:
            raise GenericException(traceback.format_exc())

    def get_secret_name_by_id(self, secret_id: str) -> str:
        """
        Trova il nome del secret by id
        :param secret_id: id del secret
        :return: nome del secret
        """
        try:
            return self.client.describe_secret(SecretId=secret_id)["Name"]
        except Exception:
            raise GenericException(traceback.format_exc())
