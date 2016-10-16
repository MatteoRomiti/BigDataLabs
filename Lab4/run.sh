# Remove folders of the previous run
hdfs dfs -rm -r data
hdfs dfs -rm -r lab4_out
hdfs dfs -rm -r lab4_out2


# Put input data collection into hdfs
hdfs dfs -put data


# Run application
hadoop jar target/Lab4-1.0.0.jar it.polito.bigdata.hadoop.lab4.DriverBigData 1 "data/*.csv"  lab4_out lab4_out2



