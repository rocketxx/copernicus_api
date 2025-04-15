import os
from dotenv import load_dotenv
from pathlib import Path

# Carica le variabili d'ambiente dal file .env
load_dotenv(Path(__file__).parent.parent / '.env')

class Config:
    COPERNICUS_USERNAME = os.getenv('COPERNICUS_USERNAME')
    COPERNICUS_PASSWORD = os.getenv('COPERNICUS_PASSWORD')

    @staticmethod
    def validate_credentials():
        if not Config.COPERNICUS_USERNAME or not Config.COPERNICUS_PASSWORD:
            raise ValueError(
                "Credenziali mancanti! Assicurati di aver configurato "
                "COPERNICUS_USERNAME e COPERNICUS_PASSWORD nel file .env"
            )