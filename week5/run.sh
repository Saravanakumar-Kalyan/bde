 hadoop dfs -rm -r /user/diamond_output*
 hadoop dfs -rm -r /user/diamond_input
 javac -cp $(hadoop classpath) DiamondCounterPartition.java
 mv *.class classes
 jar cvf diamond1.jar classes/*
 hadoop dfs -mkdir /user/diamond_input
 hadoop dfs -put input.txt /user/diamond_input/
#  hadoop dfs -ls /user/diamond_input
 hadoop jar diamond1.jar DiamondCounterPartition /user/diamond_input /user/diamond_output 2
#  hadoop dfs -cat /user/diamond_output/*
#  hadoop dfs -ls /user
hadoop dfs -cat /user/diamond_output/*