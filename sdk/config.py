import json

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_yaml import parse_yaml_file_as


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    private_key: str = ""
    akave_sdk_node: str = "connect.akave.ai:5000"
    akave_ipc_node: str = "connect.akave.ai:5500"
    ethereum_node_url: str = (
        "https://n3-us.akave.ai/ext/bc/2JMWNmZbYvWcJRPPy1siaDBZaDGTDAaqXoY5UBKh4YrhNFzEce/rpc"
    )
    storage_contract_address: str = "0x9Aa8ff1604280d66577ecB5051a3833a983Ca3aF"
    access_contract_address: str = ""

    @classmethod
    def load_config(cls, file_path: str = "config.yaml") -> "Config":
        try:
            return parse_yaml_file_as(cls, file_path)
        except FileNotFoundError:
            try:
                with open(file_path, "r") as file:
                    data = json.load(file)
                    return cls(**data)
            except FileNotFoundError:
                return cls()


config = Config.load_config()
