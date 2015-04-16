#!/bin/bash


export HADOOP_CLASSPATH=./target/classes/

echo $HADOOP_CLASSPATH

hadoop MaxValue -conf conf/hadoop-local.xml input/input-data.txt output
