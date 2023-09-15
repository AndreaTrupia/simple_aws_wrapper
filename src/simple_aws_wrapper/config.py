from __future__ import annotations

import boto3

from src.simple_aws_wrapper.const.regions import Region


class AWSConfig:
    """
    Implementazione generica delle API di AWS per manipolare risorse in cloud.
    """

    __region: Region
    __endpoint_url: str | None = None
    __configured: bool = False

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(AWSConfig, cls).__new__(cls)
        return cls.instance

    def is_configured(self) -> bool:
        """
        Restituisce se esiste la configurazione minima per AWS
        :return: True se esiste, False altrimenti
        """
        return self.__configured

    def set_region(self, region: Region):
        """
        Imposta la regione in cui si vuole utilizzare il ResourceManager
        :param region: regione in cui si vuole utilizzare il ResourceManager
        """
        if not isinstance(region, Region):
            raise TypeError("region must be a Region")
        if region == "":
            raise ValueError("region cannot be empty")
        self.__region = region
        self.__configured = True
        return self

    def set_endpoint_url(self, endpoint_url: str | None):
        """
        Imposta l'endpoint url in cui si vuole utilizzare il ResourceManager
        :param endpoint_url: endpoint url in cui si vuole utilizzare il ResourceManager
        """
        if endpoint_url is None or endpoint_url == "":
            raise ValueError("endpoint_url is required")
        if not isinstance(endpoint_url, str):
            raise TypeError("endpoint_url must be a string")
        if endpoint_url == "":
            raise ValueError("endpoint_url cannot be empty")
        self.__endpoint_url = endpoint_url
        return self

    def get_endpoint_url(self) -> str | None:
        """
        Restituisce l'endpoint url utilizzato per il ResourceManager
        :return: endpoint url utilizzato per il ResourceManager
        """
        return self.__endpoint_url

    def get_region(self) -> Region:
        """
        Restituisce la regione utilizzata per il ResourceManager
        :return: regione utilizzata per il ResourceManager
        """
        return self.__region

    def get_region_name(self) -> str:
        """
        Restituisce la regione utilizzata per il ResourceManager
        :return: regione utilizzata per il ResourceManager
        """
        return self.__region.get_region_name()

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