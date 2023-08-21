import os
from dotenv import load_dotenv


class SecretProvider:
    def __init__(self):
        load_dotenv()

    def get_secret(self, secret_name: str) -> str:
        return os.environ.get(secret_name)
