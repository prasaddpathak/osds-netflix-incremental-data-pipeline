### Instructions to setup Airflow

1. Download the airflow docker recipe `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.0/docker-compose.yaml'`
2. Create airflow folders `mkdir dags logs plugins`
3. Build the image `docker-compose up airflow-init`
4. Run the container `docker-compose up`
