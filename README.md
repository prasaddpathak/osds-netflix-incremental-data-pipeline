### Instructions to setup Airflow

1. Download the airflow docker recipe `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.0/docker-compose.yaml'`
2. Create airflow folders `mkdir dags logs plugins`
3. Build the image `docker-compose build`
4. Airflow init `docker-compose up airflow-init`
5. Run the container `docker-compose up -d`


### Instructions to setup Trino

1. Download the docker image `docker pull trinodb/trino`
2. Spin up a Trino container `docker run -p 8090:8080 --name trino trinodb/trino`
