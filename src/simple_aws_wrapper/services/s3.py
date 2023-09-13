from __future__ import annotations

from src.simple_aws_wrapper.AWS import AWS
from src.simple_aws_wrapper.enums import services
from src.simple_aws_wrapper.exceptions.generic_exception import GenericException


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
        s3 = AWS.get_client(services.S3, self.region_name, self.endpoint_url)
        try:
            s3.put_object(Body=body, Bucket=bucket_name, Key=object_key)
            return True
        except Exception as e:
            print(str(e))
            raise GenericException
