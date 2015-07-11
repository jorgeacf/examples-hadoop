#!/bin/bash

echo ""
echo "Hadoop Job Runner Script"
echo ""
echo "Parameter 1: Job class name"
echo "Parameter 2: Running mode (1-local, 2-pseudo-distributed)"
echo ""

#clear

#mvn compile

export HADOOP_CLASSPATH=./target/classes/

if [ "$2" = "" ]; then
	$2 = 1
fi

if [ "$2" = "1" ]; then # LOCAL MODE
	echo "1111"
fi

if [ "$2" = "2" ]; then # PSEUDO-DISTRIBUTED
	echo "2222"
fi


#mvn compile

#clear

#export HADOOP_CLASSPATH=./target/classes/

#$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash input
#$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash output

#$HADOOP_PATH/bin/hdfs dfs -put input input

#$HADOOP_PATH/bin/hdfs dfs -mkdir cache
#$HADOOP_PATH/bin/hadoop dfs -put distributed_cache/file1.txt cache/file1.txt
#$HADOOP_PATH/bin/hadoop com.jorgefigueiredo.hadoop.demos.jobs.JobWithDistributedCache

#$HADOOP_PATH/bin/hadoop com.jorgefigueiredo.hadoop.demos.jobs.JobCounters

#$HADOOP_PATH/bin/hdfs dfs -get output output

#cat output/part*