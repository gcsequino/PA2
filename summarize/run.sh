#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA2/out_sum
$HADOOP_HOME/bin/hadoop jar Sum.jar Sum -D mapreduce.framework.name=yarn /PA2/in_demo /PA2/out_sum
rm ./out.txt
$HADOOP_HOME/bin/hadoop fs -cat /PA2/out_sum/* >> ./out.txt
$HADOOP_HOME/bin/hadoop fs -put ./out.txt /PA2/out_sum/