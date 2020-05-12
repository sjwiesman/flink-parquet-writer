# Flink Parquet Writer

A simple Flink job that reads Avro records from Kafka and writes the data as parquet.

## Command Line Arguments

| Flags | Description |
|-------|------------|
|generate-data | When set the Job will generate data in memory instead of consuming from Kafka |
| topic | The Kafka topic to consume data from |
| bootstrap.servers | A comma separated list of Kafka brokers |
| output | The output topic level directory to write parquet |

## Usage 

```bash
$ mvn generate-sources
$ mvn package
$ /bin/flink run flink-parquet-writer.jar --bootstrap.servers localhost:8080 --topic user-input --output hdfs://users/
```
