#!/bin/bash

# Check if all parameters are passed
if [ $# -ne 3 ]; then
    echo "Usage: ./automater.sh <param1> <param2> <param3>"
    exit 1
fi

# Generate the package
mvn package

# Copy package to docker node master apps
docker cp target/tead-1.0.jar node-master:/apps

# Submit job to hadoop
docker exec node-master hadoop jar /apps/tead-1.0.jar $1 $2 $3

# Get results from the job
docker exec node-master hadoop fs -get $3

# Copy to to local machine
docker cp node-master:$3 .

# Delete result files
docker exec node-master hadoop fs -rm -r $3
docker exec node-master rm -r $3
