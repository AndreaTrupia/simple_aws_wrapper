from __future__ import annotations

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
        output_dict: dict = {}
        for parameter in parameters_list:
            try:
                output_dict[parameter] = self.client.get_parameters(
                    Names=[parameter], WithDecryption=True
                )["Parameters"][0]["Value"]
            except:
                print(
                    f"Errore nel recupero della chiave {parameter} dal parameter store"
                )
                output_dict[parameter] = ""
                raise GenericException
        return output_dict
