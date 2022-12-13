#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main IDF.java
jar cf IDF.jar IDF*.class