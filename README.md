# HalltapeETLPipeline
***
![HalltapeETLPipeline](https://github.com/halltape/HalltapeETL/blob/main/png/etl.png)
## Getting Started

### Building the Docker Images

To build the Docker images, navigate to the root directory of the project in your terminal and run the following command in background:
```bash
docker-compose up -d
```

To show all the running docker containers
```bash
docker ps
```

To stop all the docker containers
```bash
docker-compose down
```

To go inside the docker container
```bash
docker exec -it <containerID> bash
```

This project sets up a Docker environment with Airflow, PostgreSQL, and ClickHouse. The services are orchestrated using Docker Compose.

***
## Prerequisites

- Docker: Make sure you have Docker installed on your system. You can download it from [here](https://www.docker.com/products/docker-desktop).
- Docker Compose: Also ensure that Docker Compose is installed. You can find it [here](https://docs.docker.com/compose/install/).

## Services

| Service | Port | User    | Password |
|---------|------|---------|----------|
| Airflow | http://localhost:8080 |   airflow      |    airflow      |
| PostgreSQL | http://localhost:5432 | airflow | airflow         |
| ClickHouse | http://localhost:9000 |  airflow       |   airflow       |


### PostgreSQL

The PostgreSQL service is used as the backend database for Airflow. The service uses the `postgres:15` image and exposes the default PostgreSQL port `5432`. The data for the service is persisted in the `postgres-db-volume` volume.

### ClickHouse

The ClickHouse service is used for data storage and querying. The service uses the `yandex/clickhouse-server:latest` image and exposes the default ClickHouse ports `8123` and `9000`. The data for the service is persisted in the `./clickhouse_data` and `./clickhouse_logs` volumes.

### Airflow

The Airflow service is split into three separate services for the webserver, scheduler, and worker. Each service is built from the `Dockerfile` in the `./airflow_docker_image` directory. The services share common environment variables and volumes defined in the `x-airflow-common` section.
