
mvn clean compile package

#clear

export HADOOP_CLASSPATH=./target/classes/  # Hadoop ClassPath
#export HADOOP_MAPRED_HOME=$HADOOP_PATH

#export HADOOP_MAPRED_HOME=$HADOOP_MAPRED_HOME:./target/classes/
##export YARN_HOME=./target/classes/

$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash /user/jorgeacf/input
$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash /user/jorgeacf/output

$HADOOP_PATH/bin/hdfs dfs -mkdir /user/jorgeacf/input
$HADOOP_PATH/bin/hdfs dfs -put input /user/jorgeacf/

#$HADOOP_PATH/bin/hdfs dfs -mkdir cache
#$HADOOP_PATH/bin/hdfs dfs -put cache/file1.txt cache/file1.txt
#$HADOOP_PATH/bin/hadoop com.jorgefigueiredo.hadoop.demos.jobs.JobWithDistributedCache

cp target/hadoop_demos-1.0.jar /opt/apache_hadoop/share/hadoop/yarn/

$HADOOP_PATH/bin/hadoop jar $HADOOP_PATH/share/hadoop/yarn/hadoop_demos-1.0.jar com.jorgefigueiredo.demos.hadoop.jobs.JobCounters --jar $HADOOP_PATH/share/hadoop/yarn/hadoop_demos-1.0.jar --shell_command date --num_containers 1

rm /opt/apache_hadoop/share/hadoop/yarn/hadoop_demos-1.0.jar

rm -r output

$HADOOP_PATH/bin/hdfs dfs -get output output

cat output/part*




 #$HADOOP_PATH/bin/hadoop jar $HADOOP_PATH/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.0.jar org.apache.hadoop.yarn.applications.distributedshell.Client --jar $HADOOP_PATH/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.0.jar --shell_command date --num_containers 1 --master_memory 123
