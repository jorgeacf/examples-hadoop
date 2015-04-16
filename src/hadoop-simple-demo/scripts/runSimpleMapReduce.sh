#!/bin/bash

export HADOOP_CLASSPATH=./target/classes/

rm -r output

hadoop MaxValue input/input-data.txt output
