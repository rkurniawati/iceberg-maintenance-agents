import json
import urllib.parse
from typing import Union, Any, Tuple, Set

from pyiceberg.table import Table
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestFile
from pyarrow import Table as PyArrowTable, ArrowInvalid

from ...config import iceberg_config
from pyiceberg.catalog import load_catalog
import boto3
from botocore.client import Config
from urllib.parse import urlparse

def get_table_schema(table_name: str) -> str:
    """
    Get the table schema for a given table name.

    Args:
        table_name (str): the name of the table

    Returns:
        The table's schema in JSON format, for example
        {
          "type": "struct",
          "fields": [
            {
              "id": 1,
              "name": "name",
              "type": "string",
              "required": false
            },
            {
              "id": 2,
              "name": "owner",
              "type": "string",
              "required": false
            }
          ],
          "schema-id": 0,
          "identifier-field-ids": ["id"]
        }
    """

    catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier: Tuple[str, str] = (iceberg_config.schema, table_name)
    table: Table = catalog.load_table(table_identifier)
    iceberg_schema: Schema = table.schema()

    # The to_dict() method provides a JSON-serializable dictionary
    json_schema: str = iceberg_schema.model_dump_json(indent=2)
    return json_schema

def get_tables() -> list[str]:
    """
    Get the list of tables in the database

    Returns:
        A list of table names
    """
    catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    return [t[1] for t in catalog.list_tables(namespace=iceberg_config.schema)]


def get_all_current_table_files(table_name: str) -> list[dict[str, Union[str, int]]]:
    """
    Loads an Iceberg table and return a list of data and delete files from the current snapshot

    Args:
        table_name (str): the name of the table

    Returns:
        A list of dictionaries containing the data file information: file_path, file_size_in_bytes, record_count, partition
    """
    try:
        # Load the catalog
        catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)

        # Load the table
        table_identifier: Tuple[str, str] = (iceberg_config.schema, table_name)
        table: Table = catalog.load_table(table_identifier)

        # Get the current snapshot
        current_snapshot: Union[Snapshot, None] = table.current_snapshot()

        if current_snapshot is None:
            print(f"Table {iceberg_config.catalog_name}.{table_name} has no snapshots (no data files).")
            return []

        # Iterate over data files in the snapshot
        # The 'files' method returns a list of DataFile objects
        data_files: PyArrowTable = table.inspect.files() # This method simplifies getting all files from current snapshot

        if not data_files:
            print(f"No data files found in the current snapshot: {current_snapshot.snapshot_id}")
            return []

        # convert data_files (pyarrow.Table to a map)
        file_infos: list[dict[str, Union[str, int]]] = []
        for df in data_files.to_pylist():
            file_infos.append(
                {
                    "file_path": df["file_path"],
                    "file_size_in_bytes": df["file_size_in_bytes"],
                    "record_count": df["record_count"],
                    "partition": str(df["partition"]),
                }
            )
        
        return file_infos

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def get_all_table_files(table_name: str) -> set[str]:
    """
    Extracts all data file paths referenced by an Iceberg table from all snapshots.

    Args:
        table_name (str): the name of the table

    Returns:
        A list of dictionaries containing the data file paths
    """
    # Load the catalog (e.g., 'glue' for AWS Glue, 'rest' for Iceberg REST catalog)
    catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)

    # Load the specific table
    table_identifier: Tuple[str, str] = (iceberg_config.schema, table_name)
    table: Table = catalog.load_table(table_identifier)

    try:
        all_files: PyArrowTable = table.inspect.all_files()

        return {x["file_path"] for x in all_files.to_pylist()}
    except ArrowInvalid as e:
        # empty table will result in ArrowInvalid
        return set()
    except Exception as e:
        print(f"An error occurred: {e}")
        return set()


def get_table_location(table_name: str) -> str:
    """
    Get the location of a table
    Args:
        table_name (str): the name of the table
    Returns:
        The location of the table
    """
    catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier = (iceberg_config.schema, table_name)
    table: Table = catalog.load_table(table_identifier)
    return table.location()
 
def list_s3_files(bucket: str, prefix: str) -> Set[str]:
    """
    Lists all files in a specific S3 bucket and prefix.

    Args:
        bucket (str): the name of the bucket
        prefix (str): the name of the prefix

    Returns:
        A list of dictionaries containing the data file paths
    """
    s3_client_config: Config = Config(
        s3={'addressing_style': 'path'}
    )
    s3: Any = boto3.client(
        's3',
        endpoint_url=iceberg_config.catalog_properties["s3.endpoint"],
        aws_access_key_id=iceberg_config.catalog_properties["s3.access-key-id"],
        aws_secret_access_key=iceberg_config.catalog_properties["s3.secret-access-key"],
        region_name=iceberg_config.catalog_properties["s3.region"],
        config=s3_client_config
    )
    paginator: Any = s3.get_paginator('list_objects_v2')
    pages: Any = paginator.paginate(Bucket=bucket, Prefix=prefix)

    s3_files: Set[str] = set()
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Construct the full s3 path (s3://bucket/key)
                full_path: str = f"s3://{bucket}/{obj['Key']}"
                s3_files.add(full_path)
    return s3_files

def find_orphan_files(table_name: str) -> list[str]:
    """
    Identifies orphans or orphan files in a given table data area.
    Orphan files are files that are present in a table's S3 data location but not in the Iceberg table metadata.

    Args:
        table_name (str): the name of the table

    Returns:
        A set containing the orphan file paths
    """

    # 1. Get referenced files from the Iceberg table
    referenced_files: Set[str] = get_all_table_files(table_name)
    table_location: str = get_table_location(table_name) + "/data"

    # 2. List all files in the S3 storage location
    # Parse the table location to get the bucket and prefix
    parsed_url: urllib.parse.ParseResult = urlparse(table_location)
    bucket_name: str = parsed_url.netloc
    prefix: str = parsed_url.path.strip('/') + '/' # Ensure it has a trailing slash

    all_s3_files: Set[str] = list_s3_files(bucket_name, prefix)

    # 3. Find the difference (Orphan Files = All S3 Files - Referenced Files)
    # The comparison should ignore metadata files (.json, .avro, etc.) as they are managed differently
    data_files_in_s3: Set[str] = {f for f in all_s3_files if '/data/' in f and not f.endswith('/')}

    orphan_files: Set[str] = data_files_in_s3 - referenced_files

    return list(orphan_files)

def get_table_metadata(table_name: str) -> dict:
    """
    Retrieves key metadata information for an Iceberg table.

    Args:
        table_name: The table name

    Returns:
        A dictionary containing various metadata attributes of the table.
        Example: {
            "Table Identifier": "dogs.dog",
            "UUID": "466fd2be-de0d-45b2-9a8d-8396967da23c",
            "Location": "s3: //warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50",
            "Current Metadata Location File": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/00005-b3a9df64-107d-4070-8198-fe5d0c6fe123.metadata.json",
            "Schema Fields Count": 2,
            "Partition Strategy": "[]",
            "Properties": {
                "write.format.default": "PARQUET"
            },
            "Current Snapshot ID": 2630786999797468000,
            "Current Manifest List Location": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/snap-2630786999797468000-1-5602c9a9-4ee9-4ad8-9684-962eb6a56cd9.avro"
        }
    """
 
    catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier: Tuple[str, str] = (iceberg_config.schema, table_name)
    table: Table = catalog.load_table(table_identifier)
 
    metadata_info: dict[str, Union[str, int, dict[str, str]]] = {
        "Table Identifier": ".".join(table_identifier),
        "UUID": str(table.metadata.table_uuid),
        "Location": table.location(),
        "Current Metadata File": table.metadata_location,
        "Schema Fields Count": len(table.schema().fields),
        "Partition Strategy": str(table.spec()),
        "Properties": table.properties,
    }
 
    # Add snapshot details if available
    current_snapshot: Union[Snapshot, None] = table.current_snapshot()
    if current_snapshot:
        metadata_info["Current Snapshot ID"] = current_snapshot.snapshot_id
        metadata_info["Current Manifest List Location"] = current_snapshot.manifest_list
    else:
        metadata_info["Current Snapshot ID"] = "N/A (Table empty or no snapshots)"
        metadata_info["Current Manifest List Location"] = "N/A"
 
    return metadata_info
 
def get_manifest_files(table_name: str) -> list[dict[str, Union[str, int]]]:
    """
    List all metadata files used in a table's current snapshot.

    Args:
        table_name: The table name

    Returns:
        A list containing a dictionary containing the metadata file paths and size
        Example:
            [
                {
                    "file_path": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/5602c9a9-4ee9-4ad8-9684-962eb6a56cd9-m0.avro",
                    "file_size_in_bytes": 7058
                },
                {
                    "file_path": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/4cc80bbc-103f-460c-a2a5-7b164d8d0175-m0.avro",
                    "file_size_in_bytes": 7061
                },
                {
                    "file_path": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/d66bc88e-686a-45aa-b724-92db49ad3508-m0.avro",
                    "file_size_in_bytes": 7061
                },
                {
                    "file_path": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/e1807c31-d0a5-4163-947e-b360a00f604e-m0.avro",
                    "file_size_in_bytes": 7062
                }
            ]
    """
    catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier: Tuple[str, str] = (iceberg_config.schema, table_name)
    table: Table = catalog.load_table(table_identifier)
 
    # Get the current snapshot
    metadata_files: list[dict[str, Union[str, int]]] = []
    if snapshot := table.current_snapshot():
        # Get the manifest list file path
 
        # Get all manifest files from the snapshot
        io: FileIO = table.io
        manifest_list: list[ManifestFile] = snapshot.manifests(io)
 
        for manifest in manifest_list:
            metadata_files.append({
              "file_path" : manifest.manifest_path,
              "file_size_in_bytes" : manifest.manifest_length,
            })
 
    return metadata_files
 
def get_snapshots(table_name: str) -> list[dict[str, Union[int, str]]]:
    """
    Get a list of all snapshots for a given table.

    Args:
        table_name: The table name

    Returns:
        A list of dictionaries containing the metadata file paths and size

        Example:
            [
                {
                    "id": 5822070522663367797,
                    "manifest_list": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/snap-5822070522663367797-1-7a9826c3-7422-46ab-91ee-e133bd40608c.avro"
                },
                {
                    "id": 8862261516233699123,
                    "manifest_list": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/snap-8862261516233699123-1-e1807c31-d0a5-4163-947e-b360a00f604e.avro"
                },
                {
                    "id": 1111354635998942017,
                    "manifest_list": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/snap-1111354635998942017-1-d66bc88e-686a-45aa-b724-92db49ad3508.avro"
                },
                {
                    "id": 5994137082587940678,
                    "manifest_list": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/snap-5994137082587940678-1-4cc80bbc-103f-460c-a2a5-7b164d8d0175.avro"
                },
                {
                    "id": 2630786999797468000,
                    "manifest_list": "s3://warehouse/dogs/dog-559ad0b6a3614546b1d4dded36f21f50/metadata/snap-2630786999797468000-1-5602c9a9-4ee9-4ad8-9684-962eb6a56cd9.avro"
                }
            ]
    """
    catalog: Catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier: Tuple[str, str] = (iceberg_config.schema, table_name)
    table: Table = catalog.load_table(table_identifier)
 
    # Get all snapshots as a list
    snapshots: list[Snapshot] = table.snapshots()
 
    # Iterate through snapshots
    list_of_snapshots: list[dict[str, Union[int, str]]] = []
    for snapshot in snapshots:
        list_of_snapshots.append({
            "id" : snapshot.snapshot_id,
            "manifest_list" : snapshot.manifest_list,
        })
 
    return list_of_snapshots

if __name__ == "__main__":
    print(find_orphan_files("shelters"))