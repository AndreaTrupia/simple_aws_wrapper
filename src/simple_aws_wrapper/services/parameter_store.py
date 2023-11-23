from __future__ import annotations

import traceback

from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.const import services
from simple_aws_wrapper.exceptions.exceptions import (
    MissingConfigurationException,
    GenericException,
)
from simple_aws_wrapper.resource_manager import ResourceManager


class ParameterStore:
    """
    Classe per la gestione del servizio ParameterStore di AWS
    """

    class Type:
        """
        Classe simil-enum per la gestione dei tipi di dato all'interno dei valori del Parameter Store
        """

        STRING = "String"
        STRING_LIST = "StringList"
        SECURE_STRING = "SecureString"

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.client = ResourceManager.get_client(services.SSM, **AWSConfig().to_dict())

    def get_parameters_values_from_list(self, parameters_list: list) -> dict:
        """
        Funzione per il recupero dei valori dal servizio Parameter Store a partire dalla lista dei nomi dei parametri
        da recuperare
        :param parameters_list: lista dei nomi dei parametri di cui recuperare il valore
        :return: dizionario {"<nome_parametro>": "<valore_parametro>"}
        """
        try:
            output_dict: dict = {}
            if len(parameters_list) == 0:
                return {}
            if len(parameters_list) > 10:
                output_dict = dict(
                    output_dict,
                    **self.get_parameters_values_from_list(parameters_list[0:10]),
                )
                output_dict = dict(
                    output_dict,
                    **self.get_parameters_values_from_list(parameters_list[10:]),
                )
            else:
                print(f"Attempting to retrieve following parameters from Parameter Store:{parameters_list}")
                response = self.client.get_parameters(
                    Names=parameters_list, WithDecryption=True
                )["Parameters"]
                for parameter in response:
                    output_dict[parameter["Name"]] = parameter["Value"]
            return output_dict
        except Exception:
            raise GenericException("Error retrieving parameters from Parameter Store. \n" + traceback.format_exc())

    def create_parameter(self, key: str, value: str, type: str, **kwargs) -> bool:
        """
        Funzione per la creazione di un parametro nel servizio Parameter Store
        :param key: chiave del parametro da creare
        :param value: valore del parametro da creare
        :param type: tipo del parametro da creare
        :param kwargs: opzionali
        :return: None
        """
        try:
            self.client.put_parameter(Name=key, Value=value, Type=type, **kwargs)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())

    def delete_parameter(self, key: str) -> bool:
        """
        Funzione per la cancellazione di un parametro dal servizio Parameter Store
        :param key: chiave del parametro da eliminare
        :return: None
        """
        try:
            self.client.delete_parameter(Name=key)
            return True
        except Exception:
            raise GenericException(traceback.format_exc())
