hdfs dfs -mkdir /user/join_input
hdfs dfs -put _partition.txt /user/join_input/
hdfs dfs -put *.csv /user/join_input/
hdfs dfs -ls /user/join_input
mkdir classes
javac -cp $(hadoop classpath) -d classes/ *.java
jar cvf join.jar classes/*
hadoop jar join.jar JoinDriver /user/join_input/customers.csv /user/join_input/orders.csv /user/join_input/_partition.txt /user/join_output
hdfs dfs -ls /user/join_output
hdfs dfs -cat /user/join_output/*
hdfs dfs -cat /user/join_output/part-r-00000
hdfs dfs -cat /user/join_output/part-r-00001
hdfs dfs -cat /user/join_output/part-r-00002
