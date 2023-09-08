from __future__ import annotations

import boto3


class AWS:
    """
    Implementazione generica delle API di AWS per manipolare risorse in cloud.
    """

    @staticmethod
    def get_client(service_name: str, region_name: str, endpoint_url: str):
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


class S3:
    """
    Classe per la gestione di bucket S3 su AWS
    """

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def put_object(self, body: bytes, bucket_name: str, object_key: str):
        """
        Funzione per inserire un oggetto all'interno di un bucket
        :param body: contenuto del file codificato in byte
        :param bucket_name: nome del buket su cui effettuare l'upload
        :param object_key: objectkey per identificare l'oggetto all'interno del bucket
        :return: None
        """
        s3 = AWS.get_client("s3", self.region_name, self.endpoint_url)
        s3.put_object(Body=body, Bucket=bucket_name, Key=object_key)


class DynamoDB:
    """
    Classe per la gestione di DynamoDB su AWS
    """

    def __init__(self, region_name: str, endpoint_url: str | None = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url

    def put_item(self, table_name: str, item: dict):
        """
        Funzione per inserire una enry all'interno di una tabella
        :param table_name: nome della tabella in cui effettuare l'inserimento
        :param item: entry da inserire sotto forma di dizionario chiave-valore
        :return: None
        """
        if self.endpoint_url and self.endpoint_url != "":
            dynamodb_table = AWS.get_resource(
                "dynamodb", self.region_name, self.endpoint_url
            ).Table(table_name)
        else:
            dynamodb_table = AWS.get_resource("dynamodb", self.region_name).Table(
                table_name
            )
        dynamodb_table.put_item(Item=item)


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

    def send_message(self, queue_name: str, message_body: str | dict):
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
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
        )
