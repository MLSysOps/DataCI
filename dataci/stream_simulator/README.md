# Streaming Data Simulator
This is a simple stream data simulator that generates data streams from a time-series dataset.

# Install
We need to set up the data sink as a storage backend for the streams. We choose an Online Analytical Processing (OLAP)
Database (i.e., Hive) to do so. We set up the [Hive OLAP service](https://hive.apache.org/developement/quickstart/) using Docker.

```shell
export HIVE_VERSION=4.0.0-alpha-2
mkdir "${HOME}/warehouse"

docker run -d -p 10000:10000 -p 10002:10002 \
  -v ${HOME}/warehouse:/opt/hive/data/warehouse \
  --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:${HIVE_VERSION}
```

Now we can test the connection by:
1. Beeline
    ```shell
    docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'
    ```
2. HiveServer2 Web UI
   Accessed on browser at [`http://localhost:10002/`](http://localhost:10002/)

After save a dataset, it saves as an external table to the Hive metastore.
The dataset stores as an internal table in the Hive warehouse when published.
