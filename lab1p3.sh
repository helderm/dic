#!/bin/bash

# start the daemons
hadoop-daemon.sh start namenode
hadoop-daemon.sh start datanode

# clean the output dir
hdfs dfs -rmdir output/_temporary/0 --ignore-fail-on-non-empty &>/dev/null
hdfs dfs -rmdir output/_temporary/ --ignore-fail-on-non-empty &>/dev/null
hdfs dfs -rm output/* &>/dev/null
hdfs dfs -rmdir output/ &>/dev/null

hdfs dfs -ls output/

# run hadoop
hadoop jar out/artifacts/l1p3_jar/dic.jar org.helderm.dic.TopTen input/users.xml output

# list the output
hdfs dfs -cat output/part-r-00000

