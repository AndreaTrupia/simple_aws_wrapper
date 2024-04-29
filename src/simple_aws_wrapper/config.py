from __future__ import annotations

from simple_aws_wrapper.const.regions import Region


class AWSConfig:
    """
    Classe per la gestione della configurazione di AWS
    """

    __region: Region
    __endpoint_url: str | None = None
    __configured: bool = False
    __aws_secret_access_key = None
    __aws_access_key_id = None
    __aws_session_token = None

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
        if not isinstance(region, Region) and isinstance(region, str):
            region = Region(region)
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

    def set_aws_access_key_id(self, aws_access_key_id: str | None):
        """
        Imposta l'access key id utilizzato per il ResourceManager
        :param aws_access_key_id: access key id utilizzato per il ResourceManager
        """
        if aws_access_key_id is None or aws_access_key_id == "":
            raise ValueError("aws_access_key_id is required")
        if not isinstance(aws_access_key_id, str):
            raise TypeError("aws_access_key_id must be a string")
        self.__aws_access_key_id = aws_access_key_id
        return self

    def set_aws_secret_access_key(self, aws_secret_access_key: str | None):
        """
        Imposta la secret access key utilizzata per il ResourceManager
        :param aws_secret_access_key: secret access key utilizzata per il ResourceManager
        """
        if aws_secret_access_key is None or aws_secret_access_key == "":
            raise ValueError("aws_secret_access_key is required")
        if not isinstance(aws_secret_access_key, str):
            raise TypeError("aws_secret_access_key must be a string")
        self.__aws_secret_access_key = aws_secret_access_key
        return self

    def set_aws_session_token(self, aws_session_token: str | None):
        """
        Imposta la session token utilizzata per il ResourceManager
        :param aws_session_token: session token utilizzata per il ResourceManager
        """
        if aws_session_token is None or aws_session_token == "":
            raise ValueError("aws_session_token is required")
        if not isinstance(aws_session_token, str):
            raise TypeError("aws_session_token must be a string")
        self.__aws_session_token = aws_session_token
        return self

    def get_aws_access_key_id(self) -> str | None:
        """
        Restituisce l'access key id utilizzato per il ResourceManager
        :return: access key id utilizzato per il ResourceManager
        """
        return self.__aws_access_key_id

    def get_aws_secret_access_key(self) -> str | None:
        """
        Restituisce la secret access key utilizzata per il ResourceManager
        :return: secret access key utilizzata per il ResourceManager
        """
        return self.__aws_secret_access_key

    def get_aws_session_token(self) -> str | None:
        """
        Restituisce la session token utilizzata per il ResourceManager
        :return: session token utilizzata per il ResourceManager
        """
        return self.__aws_session_token

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

    def to_dict(self) -> dict:
        """
        Restituisce la configurazione AWS come dizionario
        :return: configurazione AWS come dizionario
        """
        return {
            "region_name": self.__region.get_region_name(),
            "endpoint_url": self.__endpoint_url,
            "aws_access_key_id": self.__aws_access_key_id,
            "aws_secret_access_key": self.__aws_secret_access_key,
            "aws_session_token": self.__aws_session_token,
        }