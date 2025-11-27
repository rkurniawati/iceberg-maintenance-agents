import os
from dataclasses import dataclass
from sqlalchemy.engine import URL

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
