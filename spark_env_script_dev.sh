#!/bin/bash -l
export SPARK_HOME=/usr/lib/spark 
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-src.zip:$PYTHONPATH 
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH 
export PYSPARK_PYTHON=/usr/bin/python3
python3 "$@"