# Create a SparkSession

from pyspark.sql import SparkSession
 
spark = SparkSession.builder\
        .master("local[*]")\
        .config("spark.driver.cores", 1)\
        .appName("understanding_sparksession")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

nums = spark.sparkContext.parallelize([1,2,3,4])
squared      = nums.map(lambda x :x*x)
#squared_coll = squared.collect()
#for num in squared_coll:
#   print ("The number is %d " %(num)) 

print(type(nums))
print(type(squared))
#print(type(squared_coll))

lines = spark.sparkContext.parallelize(["hello world","hi"])
words = lines.flatMap(lambda line:line.split(" "))
#words_coll  = words.collect()
print(type(lines))
print(type(words))
#print(type(words_coll))

#print(words.first())
#for w in words_coll :
#   print(w)

rdd1 = spark.sparkContext.parallelize(["coffee","panda","coffee","monkey","tea"])
rdd2 = spark.sparkContext.parallelize(["coffee","monkey","kitty"])

#print(rdd1.distinct().collect())
#print(rdd1.union(rdd2).collect())
#print(rdd1.intersection(rdd2).collect())
#print(rdd1.subtract(rdd2).collect())
print(rdd1.cartesian(rdd2).collect())
print(nums.sample(False,0.5).collect())

sum_nums = nums.reduce(lambda x,y:x+y)
print(sum_nums)

spark.stop()
