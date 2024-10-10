Steps to execute the code:

Step 1: Copy the files to hdfs directory-
hdfs dfs -ls -copyFromLocal local/directory/path/*.csv hdfs/directory/path 

Step 2: Run the nodes and resource manager
/usr/local/hadoop/sbin/start-dfs.sh
/usr/local/hadoop/sbin/start-yarn.sh

Step 3: set Hadoop variables
export HADOOP_HOME=/usr/local/hadoop/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

Step 4: To run the code, use below command-
python final1.py

Output file name would be kp1.csv, kp2.csv,kp3.csv and kp5.csv based on all the kpi’s I have mentioned in the report.
