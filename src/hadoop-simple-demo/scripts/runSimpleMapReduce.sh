#!/bin/bash

export HADOOP_CLASSPATH=./target/classes/

rm -r output

hadoop com.jf.hadoop_examples.main.MaxValue input/input-data.txt output
