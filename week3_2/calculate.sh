javac -cp $(hadoop classpath) SgpaCgpaCalculator.java 
jar cvf sgpa_calculate.jar *.class
hadoop dfs -rm /user/sgpa_input/input
hadoop dfs -rm -R /user/sgpa_output
hadoop dfs -cat /user/table_output/part* > input
hadoop dfs -put input /user/sgpa_input
hadoop jar sgpa_calculate.jar SgpaCgpaCalculator /user/sgpa_input /user/sgpa_output
hadoop dfs -cat /user/sgpa_output/part* > output