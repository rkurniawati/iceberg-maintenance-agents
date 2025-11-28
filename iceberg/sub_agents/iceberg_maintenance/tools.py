from trino.dbapi import connect
from ...config import trino_config


def convert_file_size_to_str(file_size_in_bytes: int) -> str:
    """
    Converts a file size in bytes to a human-readable string.

    Args:
        file_size_in_bytes (int): File size in bytes.

    Returns:
        str: Human-readable file size. For example: 128MB, 1GB
    """
    if file_size_in_bytes == 0:
        return "0B"

    size_names = ("B", "KB", "MB", "GB", "TB")
    i = 0
    file_size_in_bytes_float = float(file_size_in_bytes)
    while file_size_in_bytes_float >= 1024 and i < len(size_names) - 1:
        file_size_in_bytes_float /= 1024
        i += 1
    return f"{file_size_in_bytes_float:.0f}{size_names[i]}"

def run_compaction(table_name: str, file_size_in_bytes: int):
    """
    Perform table optimization (compaction) using the threshold specified in trino_config global settings.
    This is used for rewriting the content of the specified table so that it is merged into fewer but larger files.
    If the table is partitioned, the data compaction acts separately on each partition selected for optimization.
    This operation improves read performance.

    Args:
        table_name (str): name of the table to optimize
        file_size_in_bytes (int): file size in bytes

    Returns:
        none
    """
    conn = connect(**trino_config.connection_properties)
    cur = conn.cursor()

    try:
        cur.execute(f"ALTER TABLE {table_name} EXECUTE optimize(file_size_threshold => '{convert_file_size_to_str(file_size_in_bytes)}')")
        print("Compaction command executed successfully.")
        # Fetch results if the command returns any (e.g., status message)
        if cur.description:
            rows = cur.fetchall()
            for row in rows:
                print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()

def run_optimize_manifests(table_name: str):
    """
    Perform table manifest optimizations, rewrites manifest files to cluster them by partitioning columns.
    This can be used to optimize scan planning when there are many small manifest files or when there are
    partition filters in read queries but the manifest files are not grouped by partitions

    Args:
        table_name (str): name of the table to optimize

    Returns:
        none
    """
    conn = connect(**trino_config.connection_properties)
    cur = conn.cursor()

    try:
        cur.execute(f"ALTER TABLE {table_name} EXECUTE optimize_manifests")
        print("Optimize manifests command executed successfully.")
        if cur.description:
            rows = cur.fetchall()
            for row in rows:
                print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()


def run_expire_snapshots(table_name: str, retention_days: int):
    """
    Removes all snapshots and all related metadata and data files.
    Regularly expiring snapshots is recommended to delete data files that are no longer needed,
    and to keep the size of table metadata small. The procedure affects all snapshots that are older than
    the time period configured with the retention_threshold parameter.

    Args:
        table_name (str): name of the table to optimize
        retention_days (int): number of days to retain snapshots

    Returns:
        none
    """
    conn = connect(**trino_config.connection_properties)
    cur = conn.cursor()

    try:
        cur.execute(f"ALTER TABLE {table_name} EXECUTE expire_snapshots(retention_threshold => '{retention_days}d')")
        print("Expire snapshots command executed successfully.")
        if cur.description:
            rows = cur.fetchall()
            for row in rows:
                print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()

def run_remove_orphan_files(table_name: str, retention_days: int):
    """
    Removes all files from a table’s data directory that are not linked from metadata files and that are
    older than the value of retention_threshold parameter.
    Deleting orphan files from time to time is recommended to keep size of a table’s data directory under control.

    Args:
        table_name (str): name of the table to optimize
        retention_days (int): number of days to retain orphan files

    Returns:
        none
    """
    conn = connect(**trino_config.connection_properties)
    cur = conn.cursor()

    try:
        cur.execute(f"ALTER TABLE {table_name} EXECUTE remove_orphan_files(retention_threshold => '{retention_days}d')")
        print("Remove orphan files command executed successfully.")
        if cur.description:
            rows = cur.fetchall()
            for row in rows:
                print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_compaction("dog", 128*1024*1024)