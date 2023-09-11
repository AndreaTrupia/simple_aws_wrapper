from __future__ import annotations

import boto3


class GenericException(Exception):
    """
    Eccezione generica
    """
    ...


class AWS:
    """
    Implementazione generica delle API di AWS per manipolare risorse in cloud.
    """

    @staticmethod
    def get_client(service_name: str, region_name: str, endpoint_url: str | None = None):
        """
        Funzione per instaurare una sessione Boto3. Restituisce il session client relativo al servizio
        :param service_name: servizio con cui instaurare una connessione (es. "s3" o "dynamodb")
        :param region_name: regione aws
        :param endpoint_url: eventuale url dell'endpoint dei servizi
        :return: botocore.client
        """
        session = boto3.Session()
        if endpoint_url:
            return session.client(service_name, region_name=region_name, endpoint_url=endpoint_url)
        return session.client(service_name, region_name=region_name)

    @staticmethod
    def get_resource(service_name: str, region_name: str, endpoint_url: str | None = None):
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


class S3:
    """
    Classe per la gestione di bucket S3 su AWS
    """

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def put_object(self, body: bytes, bucket_name: str, object_key: str) -> bool:
        """
        Funzione per inserire un oggetto all'interno di un bucket
        :param body: contenuto del file codificato in byte
        :param bucket_name: nome del buket su cui effettuare l'upload
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :return: bool True se l'upload è andato OK, False altrimenti
        """
        s3 = AWS.get_client("s3", self.region_name, self.endpoint_url)
        try:
            s3.put_object(Body=body, Bucket=bucket_name, Key=object_key)
            return True
        except Exception as e:
            print(str(e))
            raise GenericException

class DynamoDB:
    """
    Classe per la gestione di DynamoDB su AWS
    """

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def put_item(self, table_name: str, item: dict) -> bool:
        """
        Funzione per inserire una enry all'interno di una tabella
        :param table_name: nome della tabella in cui effettuare l'inserimento
        :param item: entry da inserire sotto forma di dizionario chiave-valore
        :return: None
        """
        if self.endpoint_url and self.endpoint_url != "":
            dynamodb_table = AWS.get_resource("dynamodb", self.region_name, self.endpoint_url).Table(table_name)
        else:
            dynamodb_table = AWS.get_resource("dynamodb", self.region_name).Table(table_name)
        try:
            dynamodb_table.put_item(Item=item)
            return True
        except Exception as e:
            print(str(e))
            raise GenericException


class SQS:
    """
    Classe per la gestione di SQS su AWS
    """

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def create_message(self, **kwargs) -> dict:
        """
        Funzione per creare un dizionario a partire dai kwargs.
        Esempio di utilizzo:
            message:dict = create_message(parametro_a='a', parametro_b=3)
        Produce un dizionario come segue:
            {"parametro_a": "a", "parametro_b": 3}
        :param kwargs: coppie chiave valore con cui popolare il dizionario
        :return: dict
        """
        output_dict: dict = {}
        for k in kwargs.keys():
            output_dict[k] = kwargs[k]
        return output_dict

    def send_message(self, queue_name: str, message_body: str | dict) -> bool:
        """
        Funzione per inviare un messaggio in una coda. Il messaggio può essere un dizionario o una stringa. Ambo i casi
        viene trattato come una stringa
        :param queue_name: nome della coda
        :param message_body: corpo del messaggio
        :return: None
        """
        if isinstance(message_body, dict):
            message_body = str(message_body)
        sqs = AWS.get_client("sqs", self.region_name, self.endpoint_url)
        try:
            queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
            )
            return True
        except Exception as e:
            print(str(e))
            raise GenericException

class ParameterStore:
    """
    Classe per la gestione del servizio ParameterStore di AWS
    """

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def get_parameters_values_from_list(self, parameters_list: list) -> dict:
        """
        Funzione per il recupero dei valori dal servizio Parameter Store a partire dalla lista dei nomi dei parametri
        da recuperare
        :param parameters_list: lista dei nomi dei parametri di cui recuperare il valore
        :return: dizionario {"<nome_parametro>": "<valore_parametro>"}
        """
        output_dict: dict = {}
        ssm = AWS.get_client("ssm", self.region_name, self.endpoint_url)
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
