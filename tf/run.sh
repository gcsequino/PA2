#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA2/out_tf
$HADOOP_HOME/bin/hadoop jar TF.jar TF -D mapreduce.framework.name=yarn /PA2/in_s /PA2/out_tf
rm ./out.txt
$HADOOP_HOME/bin/hadoop fs -cat /PA2/out_tf/* >> ./out.txt
$HADOOP_HOME/bin/hadoop fs -put ./out.txt /PA2/out_tf/