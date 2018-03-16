Create a directory with arbres.csv and the future results in it: 
hdfs dfs -mkdir /user/MyDirectory
hdfs dfs -put /home/cloudera/Desktop/arbres.csv /user/MyDirectory

Run the code in terminal:
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.6.0-cdh5.12.0.jar  \
-file "/home/cloudera/Desktop/Mapper531.py" -mapper "python /home/cloudera/Desktop/Mapper531.py" \
-file "/home/cloudera/Desktop/Reducer531.py" -reducer "python /home/cloudera/Desktop/Reducer531.py" \
-input /user/MyDirectory/* -output /user/MyDirectory/results531

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.6.0-cdh5.12.0.jar  \
-file "/home/cloudera/Desktop/Mapper532.py" -mapper "python /home/cloudera/Desktop/Mapper532.py" \
-file "/home/cloudera/Desktop/Reducer532.py" -reducer "python /home/cloudera/Desktop/Reducer532.py" \
-input /user/MyDirectory/* -output /user/MyDirectory/results532
