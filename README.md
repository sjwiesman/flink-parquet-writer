# Flink Parquet Writer

A simple Flink job that reads Avro records from Kafka and writes the data as Hive partitioned parquet on Minio.
This playground environment includes:
* Flink Job
* Kafka
* Zookeeper (for Kafka)
* A data generator
* Minio


## Usage 

```bash
$ docker-compose build
$ docker-compose up -d
```

You can then navigate localhost:9000 to see the output data.
The access key is `demo-key` and the access secret is `demo secret`.
