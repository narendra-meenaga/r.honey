library(SparkR,lib.loc="/usr/spark2.0.2/R/lib")


# The entry point into SparkR is the SparkSession which connects your R program to a Spark cluster
# master - The cluster manager to connect to
#        - local - 	Run Spark locally with one worker thread (i.e. no parallelism at all).
#        - local[K]	Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
#        - local[*]	Run Spark locally with as many worker threads as logical cores on your machine.
#        - spark://HOST:PORT	Connect to the given Spark standalone cluster master. 
#                                The port must be whichever one your master is configured to use, which is 7077 by default.
#        - yarn	         Connect to a YARN cluster in client or cluster mode depending on the value of --deploy-mode. 
#                        The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable.
#spark.driver.cores      -  Number of cores to use for the driver process, only in cluster mode
#spark.driver.memory     -  Amount of memory to use for the driver process
#spark.executor.cores    -  The number of cores to use on each executor. 
#spark.executor.memory   -  Amount of memory to use per executor process
#spark.submit.deployMode -  The deploy mode of Spark driver program, either "client" or "cluster", 
#                           Which means to launch driver program locally ("client") or remotely ("cluster") on one of the nodes inside the cluste
sparkR.session(spark.app.name = "My First Program in SparkR",
               master = "local[*]",
               spark.driver.cores = 1,
               spark.driver.memory = "2g",
               spark.executor.cores = 2,
               spark.executor.memory = "4g",
               spark.submit.deployMode = "client" )



df <- as.DataFrame(mtcars)
show(df)
showDF(df,2)


