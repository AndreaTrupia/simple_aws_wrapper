US_EAST_2 = "us-east-2"
US_EAST_1 = "us-east-1"
US_WEST_1 = "us-west-1"
US_WEST_2 = "us-west-2"
AF_SOUTH_1 = "af-south-1"
AP_EAST_1 = "ap-east-1"
AP_SOUTH_2 = "ap-south-2"
AP_SOUTHEAST_3 = "ap-southeast-3"
AP_SOUTHEAST_4 = "ap-southeast-4"
AP_SOUTH_1 = "ap-south-1"
AP_NORTHEAST_3 = "ap-northeast-3"
AP_NORTHEAST_2 = "ap-northeast-2"
AP_SOUTHEAST_1 = "ap-southeast-1"
AP_SOUTHEAST_2 = "ap-southeast-2"
AP_NORTHEAST_1 = "ap-northeast-1"
CA_CENTRAL_1 = "ca-central-1"
EU_CENTRAL_1 = "eu-central-1"
EU_WEST_1 = "eu-west-1"
EU_WEST_2 = "eu-west-2"
EU_SOUTH_1 = "eu-south-1"
EU_WEST_3 = "eu-west-3"
EU_SOUTH_2 = "eu-south-2"
EU_NORTH_1 = "eu-north-1"
EU_CENTRAL_2 = "eu-central-2"
IL_CENTRAL_1 = "il-central-1"
ME_SOUTH_1 = "me-south-1"
ME_CENTRAL_1 = "me-central-1"
SA_EAST_1 = "sa-east-1"

ALLOWED_REGIONS = [
    "us-east-2",
    "us-east-1",
    "us-west-1",
    "us-west-2",
    "af-south-1",
    "ap-east-1",
    "ap-south-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-south-1",
    "ap-northeast-3",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "ca-central-1",
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "eu-south-1",
    "eu-west-3",
    "eu-south-2",
    "eu-north-1",
    "eu-central-2",
    "il-central-1",
    "me-south-1",
    "me-central-1",
    "sa-east-1",
]


class Region:
    """
    Classe per la gestione delle regioni AWS
    """

    def __init__(self, region_name: str):
        if region_name not in ALLOWED_REGIONS:
            raise ValueError(f"Region {region_name} is not allowed")
        self.__region_name = region_name

    def get_region_name(self) -> str:
        """
        Restituisce il nome della regione
        :return: nome della regione
        """
        return self.__region_name
