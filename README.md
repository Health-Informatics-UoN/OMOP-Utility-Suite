# OMOP-Utility-Suite

OMOP Utility Suite is a streamlined web application built with FastAPI. It is designed to simplify and automate complex operations within the OMOP Common Data Model (CDM), providing an intuitive interface for data management and transformation.

## Key Features

- OMOP CDM Merging - Upsert data from one postgresql OMOP CDM database to another.

## Supported CDM Versions

While the suite is optimized for CDM v5.4, it has been successfully tested for appending data from v5.3 into v5.4 environments.

> [!NOTE]
> When migrating from v5.3 to v5.4, the application automatically handles structural differences by populating new v5.4-specific columns with NULL values.

## Installation & Setup

### Local Deployment

This project utilizes uv for lightning-fast Python dependency management.

Navigate to the project directory, build the front end files and start the server:

    ```Bash
    cd frontend && npm run build   # outputs to backend/frontend/
    cd ../backend && uvicorn main:app
    ```

### Running with Docker

For a production-ready environment or quick testing, use Docker Compose:

    ```Bash
    docker-compose up --build
    ```
