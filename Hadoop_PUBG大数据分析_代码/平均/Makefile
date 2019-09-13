prog:   WordCount.java
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.9.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.0.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar -d ./ ./WordCount.java
	jar -cvf WordCount.jar WordCount*.class
	/usr/local/hadoop/bin/hadoop jar WordCount.jar WordCount input/MODE/input output
	/usr/local/hadoop/bin/hadoop dfs -cat output/*
clean:  
	/usr/local/hadoop/bin/hadoop dfs -rm -r  output
	
