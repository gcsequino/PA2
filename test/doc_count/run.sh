#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA2/out_doc_count
$HADOOP_HOME/bin/hadoop jar DocCount.jar DocCount -D mapreduce.framework.name=yarn /PA2/in_s /PA2/out_doc_count
rm ./out.txt
$HADOOP_HOME/bin/hadoop fs -cat /PA2/out_doc_count/* >> ./out.txt
$HADOOP_HOME/bin/hadoop fs -put ./out.txt /PA2/out_doc_count/