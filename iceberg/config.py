from dataclasses import dataclass
from sqlalchemy.engine import URL
from google.genai import types
from google.adk.models.google_llm import Gemini


@dataclass
class IcebergConfiguration:
    """"
    Configuration for Apache Iceberg
    """

    catalog_properties = {
        "uri": URL.create(
            drivername="postgresql",
            username="metastore-user",
            password="metastore-password",
            host="localhost",
            port=5432,
            database="metastore"
        ),
        "warehouse": "s3://warehouse/", 
        "type" : "sql",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio123",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    }
    catalog_name = "postgres-catalog"
    schema: str = "dogs"

    trino_properties = {
        "iceberg_catalog" : "iceberg"
    }

iceberg_config = IcebergConfiguration()

@dataclass
class TrinoConfiguration:
    """
    Configuration for Trino
    """
    connection_properties = {
        "host": "localhost",
        "port": "8080",
        "catalog" : "iceberg",
        "schema" : "dogs",
        "user": "dog"
    }

    file_size_threshold = "128MB"


trino_config = TrinoConfiguration()

def get_fast_model():
    return "gemini-2.5-flash"

def get_retry_config():
    return types.HttpRetryOptions(
        attempts=5,  # Maximum retry attempts
        exp_base=7,  # Delay multiplier
        initial_delay=1, # Initial delay before first retry (in seconds)
        http_status_codes=[429, 500, 503, 504] # Retry on these HTTP errors
    )
