# Streaming Data Simulator
This is a simple stream data simulator that generates data streams from a time-series dataset.

# Install
We need to setup the data sink as an storage backend for the streams. We choose an Online Analytical Processing (OLAP)
Database (i.e., Hive) to do so. We setup the Hive OLAP service using Docker.

```shell
export HIVE_VERSION=4.0.0-alpha-2
mkdir "${HOME}/warehouse"
```

1. Create a Docker network
   We will start multiple docker containers. A docker network is need for intercommunication among these containers
    ```shell
    docker network create -d bridge --subnet 172.168.0.0/16 dataci-net
    ```

2. Launch a RDBMS (Postgres)
    ```shell
    docker run --name postgres --network=dataci-net \
      -e POSTGRES_USER=hive -e POSTGRES_PASSWORD=password -e POSTGRES_DB=metastore_db \
      -d postgres
    ```

3. Launch Hive standalone metastore with external RDBMS
    ```shell
    docker run -d --network=dataci-net -p 9083:9083 -e SERVICE_NAME=metastore -e DB_DRIVER=postgres \
      -e SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=password" \
      -v ${HOME}/warehouse:/opt/hive/data/warehouse \
      --name metastore apache/hive:${HIVE_VERSION}
    ```

4. Launch Hive Server (HS2)
    ```shell
    docker run -d --network=dataci-net -p 10000:10000 -p 10002:10002 -e SERVICE_NAME=hiveserver2 \
      -e SERVICE_OPTS="-Dhive.metastore.uris=thrift://metastore:9083" \
      -v ${HOME}/warehouse:/opt/hive/data/warehouse \
      -e IS_RESUME="true" \
      --name hiveserver2 apache/hive:${HIVE_VERSION}
    ```

Now we can test the connection by:
1. Beeline
    ```shell
    docker exec -it hiveserver2 beeline -u 'jdbc:hive2://localhost:10000/'
    ```
2. HiveServer2 Web UI
   Accessed on browser at [`http://localhost:10002/`](http://localhost:10002/)
