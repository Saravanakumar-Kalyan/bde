hadoop dfs -rm -R /user/table_output
hadoop dfs -rm -R /user/table_int
javac -cp $(hadoop classpath) -d classes/ TableJoiner.java 
jar cvf join.jar classes/*
hadoop jar join.jar TableJoiner /user/table_join_input/Registrations.csv /user/table_join_input/StudentGradesNew.csv /user/table_join_input/SubjectInfo.csv /user/table_int /user/table_output