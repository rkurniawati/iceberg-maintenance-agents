# Trino with Iceberg Docker Compose Setup

This directory contains a `docker-compose.yml` file to run Trino with Apache Iceberg.

## Services

The `docker-compose.yml` file defines the following services:

- **trino-coordinator**: The Trino coordinator.
- **hive-metastore**: The Hive Metastore service.
- **metastore-db**: A PostgreSQL database for the Hive Metastore.
- **minio**: An S3-compatible object storage service.

## How to use

1.  **Start the services**:
    ```bash
    docker-compose up -d
    ```

2.  **Create the warehouse bucket in MinIO**:
    Open your browser and go to `http://localhost:9000`.
    Log in with the access key `minio` and the secret key `minio123`.
    Create a bucket named `warehouse`.

3.  **Connect to Trino**:
    You can use the Trino CLI to connect to the coordinator:
    ```bash
    trino --server http://localhost:8080
    ```

4.  **Create an Iceberg table**:
    ```sql
    CREATE SCHEMA iceberg.test;
    CREATE TABLE iceberg.test.my_table (
      id INT,
      data VARCHAR
    );
    INSERT INTO iceberg.test.my_table VALUES (1, 'a');
    SELECT * FROM iceberg.test.my_table;
    ```
