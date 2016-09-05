#!/bin/bash

# clean the output dir
#hdfs dfs -rm output/* &>/dev/null ; hdfs dfs -rmdir output &>/dev/null
hdfs dfs -rmdir output/_temporary/0 --ignore-fail-on-non-empty &>/dev/null
hdfs dfs -rmdir output/_temporary/ --ignore-fail-on-non-empty &>/dev/null
hdfs dfs -rm output/* &>/dev/null
hdfs dfs -rmdir output/ &>/dev/null

# run hadoop
hadoop jar out/artifacts/l1p2_jar/dic.jar org.helderm.dic.WordCount input/file0 output

# list the output
hdfs dfs -cat output/part-r-00000

