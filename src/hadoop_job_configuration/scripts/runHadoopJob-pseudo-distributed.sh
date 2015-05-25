
mvn compile

#clear

export HADOOP_CLASSPATH=./target/classes/

$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash input
$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash output

$HADOOP_PATH/bin/hdfs dfs -put input input

$HADOOP_PATH/bin/hdfs dfs -mkdir cache
$HADOOP_PATH/bin/hadoop dfs -put distributed_cache/file1.txt cache/file1.txt
#$HADOOP_PATH/bin/hadoop com.jorgefigueiredo.hadoop.demos.jobs.JobWithDistributedCache

$HADOOP_PATH/bin/hadoop com.jorgefigueiredo.hadoop.demos.jobs.JobCounters

$HADOOP_PATH/bin/hdfs dfs -get output output

cat output/part*