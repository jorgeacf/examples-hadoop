#!/bin/bash


export HADOOP_CLASSPATH=./target/classes/

echo $HADOOP_CLASSPATH

hadoop com.jf.hadoop_examples.main.MaxValue input/input-data.txt output
