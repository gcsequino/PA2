#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main DocCount.java
jar cf DocCount.jar DocCount*.class