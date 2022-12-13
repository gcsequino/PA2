#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA2/out_idf
$HADOOP_HOME/bin/hadoop jar IDF.jar IDF -D mapreduce.framework.name=yarn /PA2/out_tf /PA2/out_idf
rm ./out.txt
$HADOOP_HOME/bin/hadoop fs -cat /PA2/out_idf/* >> ./out.txt
$HADOOP_HOME/bin/hadoop fs -put ./out.txt /PA2/out_idf/