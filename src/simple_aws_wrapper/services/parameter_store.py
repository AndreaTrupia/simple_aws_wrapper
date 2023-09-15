from __future__ import annotations

from src.simple_aws_wrapper.config import AWSConfig
from src.simple_aws_wrapper.const import services
from src.simple_aws_wrapper.exceptions.exceptions import GenericException, MissingConfigurationException


class ParameterStore:
    """
    Classe per la gestione del servizio ParameterStore di AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.region_name = AWSConfig().get_region_name()
        self.endpoint_url = AWSConfig().get_endpoint_url()

    def get_parameters_values_from_list(self, parameters_list: list) -> dict:
        """
        Funzione per il recupero dei valori dal servizio Parameter Store a partire dalla lista dei nomi dei parametri
        da recuperare
        :param parameters_list: lista dei nomi dei parametri di cui recuperare il valore
        :return: dizionario {"<nome_parametro>": "<valore_parametro>"}
        """
        output_dict: dict = {}
        ssm = AWSConfig.get_client(services.SSM, self.region_name, self.endpoint_url)
        for parameter in parameters_list:
            try:
                output_dict[parameter] = ssm.get_parameters(Names=[parameter], WithDecryption=True)["Parameters"][0][
                    "Value"
                ]
            except:
                print(f"Errore nel recupero della chiave {parameter} dal parameter store")
                output_dict[parameter] = ""
                raise GenericException
        return output_dict
