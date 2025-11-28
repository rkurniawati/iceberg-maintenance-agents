from iceberg.config import iceberg_config
from pyiceberg.catalog import load_catalog

def get_table_schema(table_name:str) -> str:
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

    catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier = (iceberg_config.schema, table_name)
    table = catalog.load_table(table_identifier)
    iceberg_schema = table.schema()

    # The to_dict() method provides a JSON-serializable dictionary
    json_schema = iceberg_schema.model_dump_json(indent=2)
    return json_schema

def get_tables() -> list[str]:
    """
    Get the list of tables in the database

    Returns:
        A list of table names
    """
    catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    return [t[1] for t in catalog.list_tables(namespace=iceberg_config.schema)]


def get_data_file_info(table_name):
    """
    Loads an Iceberg table and prints the size of each data file.

    Args:
        table_name (str): the name of the table

    Returns:
        A list of dictionaries containing the data file information: file_path, file_size_in_bytes, record_count, partition
    """
    try:
        # Load the catalog
        catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)

        # Load the table
        table_identifier = (iceberg_config.schema, table_name)
        table = catalog.load_table(table_identifier)

        # Get the current snapshot
        current_snapshot = table.current_snapshot()

        if current_snapshot is None:
            print(f"Table {iceberg_config.catalog_name}.{table_name} has no snapshots (no data files).")
            return {}

        # Iterate over data files in the snapshot
        # The 'files' method returns a list of DataFile objects
        data_files = table.inspect.files() # This method simplifies getting all files from current snapshot

        if not data_files:
            print(f"No data files found in the current snapshot: {current_snapshot.snapshot_id}")
            return {}

        # convert data_files (pyarrow.Table to a map)
        file_infos = []
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
        return {}

if __name__ == "__main__":
    print(get_data_file_info("dog"))