## Cluster Command

Provide the command and all of the commandline options used to generate your csv and parquet files.

I have used the following command to run the script locally:


and the following command to run it on the cluster:

```bash
spark-submit --verbose ik-read-write-csv-2010.py --name --master yarn --deploy-mode cluster --executor-memory 4G --num-executors 4 ik-read-write-csv-2010.py
```

### Your Explanation

I selected the parameters based on the description of the cluster at 
http://192.168.1.100:8088/cluster/cluster. On the page it states that the maximum allowed memory per application is 4G and the maximum number of executors is 4.
In the book I could not find an exact reference to spark-submit parameters. I found description of partitioning (e.g. page 255).

In an additional paragraph, explain the process of how you handled bad/corrupt data during the read process.
