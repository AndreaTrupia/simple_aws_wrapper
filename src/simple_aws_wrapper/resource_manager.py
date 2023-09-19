from __future__ import annotations

import boto3


class ResourceManager:
    """
    Classe per la gestione di risorse e client su AWS
    """

    @staticmethod
    def get_client(
        service_name: str,
        region_name: str,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
    ):
        """
        Funzione per instaurare una sessione Boto3. Restituisce il session client relativo al servizio
        :param service_name: servizio con cui instaurare una connessione (es. "s3" o "dynamodb")
        :param region_name: regione aws
        :param endpoint_url: eventuale url dell'endpoint dei servizi
        :return: botocore.client
        """
        session = boto3.Session()
        return session.client(
            service_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_session_token=aws_session_token,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    @staticmethod
    def get_resource(
        service_name: str,
        region_name: str,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
    ):
        """
        Funzione per prendere una risorsa aws
        :param service_name: nome servizio (ad esempio "dynamodb")
        :param region_name: regione aws
        :param endpoint_url: eventuale endpoint a cui collegarsi
        :return: risorsa aws
        """
        return boto3.resource(
            service_name,
            region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    @staticmethod
    def get_global_client(
        service_name: str,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        region_name = None
    ):
        """
        Funzione per prendere un client global
        :param service_name: nome servizio (ad esempio "dynamodb")
        :param endpoint_url: eventuale endpoint a cui collegarsi
        :return: client global
        """
        return boto3.client(
            service_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
