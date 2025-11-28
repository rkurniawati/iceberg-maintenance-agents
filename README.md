# Iceberg maintainer and optimizer 

## Problem statement

Managing and optimizing Apache Iceberg data lakehouses can be complex due to the need for continuous maintenance tasks such as data compaction, metadata management, and schema evolution. This project aims to address these challenges by providing an automated, agent-driven solution for maintaining and optimizing Iceberg data lakes, reducing manual overhead and ensuring data integrity and performance.

## Using agents to perform maintenance

This project leverages a multi-agent system to automate the maintenance and optimization of Apache Iceberg data lakehouses. The system consists of a `root_agent` that orchestrates several specialized sub-agents:

- **`iceberg_database_info_agent`**: Gathers comprehensive information about the Iceberg database, including table schemas, data file statistics, and other relevant metadata.
- **`iceberg_maintenance`**: Executes various maintenance tasks such as data compaction, orphan file cleanup, and other optimization routines to ensure the data lakehouse remains efficient and performant.
- **`iceberg_knowledge_agent`**: Provides general knowledge and best practices related to Apache Iceberg, assisting in informed decision-making for maintenance and optimization strategies.

This agent-based approach allows for intelligent automation, where tasks are dynamically identified and executed based on the current state and needs of the Iceberg data lakehouse.

## Architecture

```mermaid
graph LR
    RootAgent --> IcebergDataLakeInfoAgent
    RootAgent --> IcebergMaintenanceAgent
    RootAgent --> IcebergKnowledgeAgent
    IcebergDataLakeInfoAgent --> get_tables
    IcebergDataLakeInfoAgent --> get_table_schema
    IcebergDataLakeInfoAgent --> get_datafile_info
    IcebergMaintenanceAgent --> run_compaction
    IcebergMaintenanceAgent --> run_expire_snapshots
    IcebergMaintenanceAgent --> run_optimize_manifests
    IcebergMaintenanceAgent --> run_remove_orphan_files
    IcebergKnowledgeAgent --> IcebergDocumentation
```

## Run a local Apache Iceberg datalake

You can use the iceberg provided in `./iceberg-docker`

To start:
```bash
cd iceberg-docker
docker-compose up -d
```

To start:
```bash
cd iceberg-docker
docker-compose stop -d --volumes
```