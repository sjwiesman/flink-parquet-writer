#!/bin/bash

echo "Waiting for Schema Registry to be available ⏳"

until curl --output /dev/null --silent --head --fail http://schema-registry:8085/; do
  echo "Waiting for Schema Registry to be available ⏳"
  sleep 5
done

java -classpath /opt/data-generator.jar com.ververica.datagen.DataGenerator
