from __future__ import annotations

import boto3


class ResourceManager:
    """
    Classe per la gestione di risorse e client su AWS
    """

    @staticmethod
    def get_client(
        service_name: str, region_name: str, endpoint_url: str | None = None
    ):
        """
        Funzione per instaurare una sessione Boto3. Restituisce il session client relativo al servizio
        :param service_name: servizio con cui instaurare una connessione (es. "s3" o "dynamodb")
        :param region_name: regione aws
        :param endpoint_url: eventuale url dell'endpoint dei servizi
        :return: botocore.client
        """
        session = boto3.Session()
        if endpoint_url:
            return session.client(
                service_name, region_name=region_name, endpoint_url=endpoint_url
            )
        return session.client(service_name, region_name=region_name)

    @staticmethod
    def get_resource(
        service_name: str, region_name: str, endpoint_url: str | None = None
    ):
        """
        Funzione per prendere una risorsa aws
        :param service_name: nome servizio (ad esempio "dynamodb")
        :param region_name: regione aws
        :param endpoint_url: eventuale endpoint a cui collegarsi
        :return: risorsa aws
        """
        if endpoint_url and endpoint_url != "":
            return boto3.resource(service_name, region_name, endpoint_url=endpoint_url)
        return boto3.resource(service_name, region_name)

    @staticmethod
    def get_global_client(service_name: str, endpoint_url: str | None = None):
        """
        Funzione per prendere un client global
        :param service_name: nome servizio (ad esempio "dynamodb")
        :param endpoint_url: eventuale endpoint a cui collegarsi
        :return: client global
        """
        if endpoint_url and endpoint_url != "":
            return boto3.client(service_name, endpoint_url=endpoint_url)
        return boto3.client(service_name)
