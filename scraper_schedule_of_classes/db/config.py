import os

import boto3
from dotenv import load_dotenv

load_dotenv()

DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
ENV = os.getenv("ENV")

# development environment: local machine.
if ENV == "dev":
    DB_USER = os.getenv("DB_USER_LOCAL")
    DB_PASSWORD = os.getenv("DB_PASSWORD_LOCAL")
    DB_HOST = "localhost"

# Production environment: lambda
elif ENV == "prod":
    PARAM_STORE_NAME_DB_USERNAME = os.getenv("PARAM_STORE_NAME_DB_USERNAME")
    PARAM_STORE_NAME_DB_PASSWORD = os.getenv("PARAM_STORE_NAME_DB_PASSWORD")

    print("before client")

    # username and password and secure strings in ssm.
    ssm_client = boto3.client("ssm")

    print("got client")

    parameter_username = ssm_client.get_parameter(
        Name=PARAM_STORE_NAME_DB_USERNAME, WithDecryption=True
    )["Parameter"]
    DB_USER = parameter_username["Value"]
    print("got param1")

    parameter_password = ssm_client.get_parameter(
        Name=PARAM_STORE_NAME_DB_PASSWORD, WithDecryption=True
    )["Parameter"]
    DB_PASSWORD = parameter_password["Value"]
    print("got param 2")

    DB_HOST = os.getenv("DB_HOST")
# Unknown environment type
else:
    raise EnvironmentError("environment type not specified in environment vars")