from pyiceberg.schema import Schema

from.config import iceberg_config
from pyiceberg.catalog import load_catalog

def get_table_schema(table_name:str) -> Schema:
    """
    Get the table schema for a given table name.

    Args:
        table_name (str): the name of the table

    Returns:
        The table's schema
    """

    catalog = load_catalog(iceberg_config.catalog_name, **iceberg_config.catalog_properties)
    table_identifier = (iceberg_config.schema, table_name)  # Replace with your table's namespace and name
    table = catalog.load_table(table_identifier)
    return table.schema()

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