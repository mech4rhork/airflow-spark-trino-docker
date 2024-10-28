# airflow-spark-trino-docker

Sources :
* https://github.com/sensei23/trino-hive-docker
* https://github.com/cordon-thiago/airflow-spark
* https://github.com/apache/superset/tree/4.0.2

## Lancement des dockers

### (Prerequis) shared_network
    
    docker network create shared_network

### Airflow + Spark + Jupyter
Dans `airflow-spark/docker/docker-airflow`

    # BUILD
    docker build --rm --build-arg PYTHON_DEPS="boto3 trino" -t docker-airflow-spark:1.10.7_3.1.2 .
    docker image prune -f

Dans `airflow-spark/docker`

    # RUN
    docker compose up -d

### MinIO + Trino
Dans `trino-hive-docker/docker-compose`

    # BUILD
    docker build -t my-hive-metastore .
    
    # RUN
    docker-compose up -d

### Superset
Dans `superset`

    # RUN
    docker-compose -f docker-compose-non-dev.yml up -d
######

## Ports des interfaces graphiques
    # trino 422
        image: 'trinodb/trino:422'

    # 8079: Trino coordinator
    #       	usr=<ANYTHING>
    #       	pwd=<NONE REQUIRED>
    #
    # 9000: MinIO Console
    #       	usr=minio
    #       	pwd=minio123
    #
    # 8181: Spark Web UI
    #
    # 8282: Airflow UI
    #
    # 8888: Jupyter Notebook (JupyterLab)
    #       	token=mytoken
    # 8088: Superset
    #

Ouvrir tous dans firefox

    for port in 8079 9000 8181 8282 8888 8088; do firefox "http://localhost:$port" & done
