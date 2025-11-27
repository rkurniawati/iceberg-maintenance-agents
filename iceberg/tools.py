from pyiceberg.schema import Schema

from.config import iceberg_config
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
    table_identifier = (iceberg_config.schema, table_name)  # Replace with your table's namespace and name
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

if __name__ == "__main__":
    print(get_tables())