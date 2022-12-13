#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Sum.java
jar cf Sum.jar Sum*.class