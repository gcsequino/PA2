#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main TFIDF.java
jar cf TFIDF.jar TFIDF*.class