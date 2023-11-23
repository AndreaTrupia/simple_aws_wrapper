from __future__ import annotations


class GenericException(Exception):
    """
    Eccezione generica
    """
    def __init__(self, message: str) -> None:
        super().__init__(message)


class MissingConfigurationException(Exception):
    """
    Eccezione per la configurazione mancante
    """

    ...
