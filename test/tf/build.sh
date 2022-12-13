#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main TF.java
jar cf TF.jar TF*.class