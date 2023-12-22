# Apache Flink
# How to run pipeline on cluster

### Step 1 Download and install Flink 

### Step 2: Go to apache flink folder and run following command
```
 ./bin/start-cluster.sh 
```
`Note: Above command will start flink cluster.`

### Step 3: Submit your jar file to cluster
```
./bin/flink run -c <package-name> <path-to-jar-file>.jar
```

```
 For Ex : ./bin/flink run -c org.flink.Main /Users/zop7917/IdeaProjects/apache-flink/build/pipeline-1.0-SNAPSHOT.jar 
```
* Above command will submit your jar file to the cluster.
### Step 4: Go to Flink Web UI by visiting http://localhost:8081

### Step 5: Stop Cluster
```
 ./bin/stop-cluster.sh 
```

