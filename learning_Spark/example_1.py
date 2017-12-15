
# Import required classes for Spark
from pyspark import SparkContext, SparkConf

appName = "Test"
master  = "local"


# in Spark 2.0 the same effects can be achieved through SparkSession, without expliciting creating SparkConf, SparkContext or SQLContext
# To create a SparkContext you first need to build a SparkConf object that contains information about your application.
conf = SparkConf().setAppName(appName).setMaster(master)
sc   = SparkContext(conf=conf)

# Set log level to ERROR(log4j.rootCategory=INFO, console by default in  /etc/spark/conf)
sc.setLogLevel("ERROR")


# Read a text file
# "file" is used to specify local file path. otherwise HDFS path is taken by default
lines = sc.textFile("file:///home/narendrameenaga3694/spark-projects/learning_Spark/input.txt")
# "count" is a "Action" 
print("Number of lines in the File is : " + str(lines.count()))

# "first" is a "Action"
print("The first line in  the file is : " + str(lines.first()))

# "filter" is a "transformation" - created another RDD(pipelinedRDD)
second_line =  lines.filter(lambda l : "second" in l)

# "collect" retrieves the entire RDD into memory  
# "collect" helps to deal the dataset locally - the entire dataset has to fit into memory on single machine to use collect
s_l  = second_line.collect()
print(s_l)

# "take" is used to retrieve a small number of elements  in the RDD at the driver program 
print(second_line.take(1))


first_line = lines.filter(lambda l : "first" in l)

# "union" operates on two RDDs 
all_lines = first_line.union(second_line)
print(all_lines.collect())


# "parallelize" may makes your collection suitable for processing on multiple nodes i.e it created RDD
nums = sc.parallelize ([1,2,3,4])
# "map"  applies a function to each emelemnt in the RDD
squared = nums.map(lambda x :x *x).collect()
for num in squared :
     print(num)

 

