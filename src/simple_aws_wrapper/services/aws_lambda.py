from simple_aws_wrapper.config import AWSConfig
from simple_aws_wrapper.exceptions.exceptions import (
    MissingConfigurationException,
    GenericException,
)
from simple_aws_wrapper.resource_manager import ResourceManager


class Lambda:
    """
    Classe per la gestione di Lambda su AWS
    """

    def __init__(self):
        if not AWSConfig().is_configured():
            raise MissingConfigurationException
        self.client = ResourceManager.get_client(
            service_name="lambda", **AWSConfig().to_dict()
        )

    def invoke(
        self,
        function_name: str,
        invocation_type: str | None = None,
        payload: dict | None = None,
        **kwargs
    ):
        """
        Funzione per l'invocazione di un Lambda
        :param function_name: nome del lambda da invocare
        :param invocation_type: tipo di invocazione (es. "RequestResponse" o "Event")
        :param payload: payload da inviare alla lambda
        :return: response
        """
        try:
            return self.client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=payload,
                **kwargs
            )
        except Exception as e:
            print(str(e))
            raise GenericException
