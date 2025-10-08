pwd >> bigdata/

 2060  javac -cp $(hadoop classpath) -d classes *.java
 2061  javac -cp $(hadoop classpath) -d classes *.java -Xlint:deprecation
 2062  ls classes

 2065  jar cvf wordcount.jar classes/*.class
 2066  clear
 2067  ls

 2077  hadoop jar WordCount/wordcount.jar WordCountDriver /user/wordcount_input /user/wordcount_output

 2082  hadoop jar WordCount/wordcount.jar WordCountDriver /user/wordcount_input /user/wordcount_output
 2085  hdfs dfs -ls /user/wordcount_output/*
 2086  hdfs dfs -cat /user/wordcount_output/*

start dfs yarn sh
stop all sh
