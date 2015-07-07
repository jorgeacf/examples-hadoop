
mvn compile package

#clear

export HADOOP_CLASSPATH=./target/classes/  # Hadoop ClassPath

#export HADOOP_MAPRED_HOME=$HADOOP_MAPRED_HOME:./target/classes/
##export YARN_HOME=./target/classes/

$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash /user/jorgeacf/input
$HADOOP_PATH/bin/hdfs dfs -rm -r -skipTrash /user/jorgeacf/output

$HADOOP_PATH/bin/hdfs dfs -mkdir /user/jorgeacf/input
$HADOOP_PATH/bin/hdfs dfs -put input /user/jorgeacf/

#$HADOOP_PATH/bin/hdfs dfs -mkdir cache
#$HADOOP_PATH/bin/hdfs dfs -put cache/file1.txt cache/file1.txt
#$HADOOP_PATH/bin/hadoop com.jorgefigueiredo.hadoop.demos.jobs.JobWithDistributedCache

#$HADOOP_PATH/bin/hdfs dfs -put ./target/hadoop_job_configuration-0.0.1-SNAPSHOT.jar hadoop_job_configuration-0.0.1-SNAPSHOT.jar

cp target/*.jar target/a.jar

cp target/a.jar /opt/apache_hadoop/share/hadoop/yarn/

$HADOOP_PATH/bin/yarn com.jorgefigueiredo.hadoop.demos.jobs.HadoopJobRunner3

rm /opt/apache_hadoop/share/hadoop/yarn/a.jar

rm -r output

$HADOOP_PATH/bin/hdfs dfs -get output output

cat output/part*