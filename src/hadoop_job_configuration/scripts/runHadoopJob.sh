
mvn compile

#clear

export HADOOP_CLASSPATH=./target/classes/

rm -r ./output

hadoop com.jorgefigueiredo.hadoop.demos.jobs.HadoopJobRunner3

cat output/part*