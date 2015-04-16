#!/bin/bash

# Compile the project with maven
#mvn compile

# Export the classes path to the variable HADOOP_CLASSPATH
export HADOOP_CLASSPATH=./target/classes/

hadoop com.jf.hadoop_examples.configuration.ConfigurationDisplay -conf src/main/java/hadoop-local.xml | grep mapred.job.tracker
