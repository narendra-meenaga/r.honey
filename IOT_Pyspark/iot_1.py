# Pyspark script to perform logistic regression using MLLib on AWS

# Global settings 
inputPath  = "/home/ubuntu/iot_project/input_data/"
inputFiles = ['normal1.csv','normal2.csv','bug1.csv']


#  SparkContext libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import NullType
from pyspark.ml.classification import LogisticRegression


# Create SparkContxt variable 'sc'
conf = SparkConf().setAppName("logistic regression on iot data")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Display only Errors
sc.setLogLevel("ERROR")

print("\n\n\n\n\n")

# Create Datframes from input files
df_normal_1 = sqlContext.read.load(inputPath + inputFiles[0],
                                   format='com.databricks.spark.csv',
                                   header='true',
                                   inferSchema='true')

df_normal_2 = sqlContext.read.load(inputPath + inputFiles[1],
                                   format='com.databricks.spark.csv',
                                   header='true',
                                   inferSchema='true')

df_bug_1    = sqlContext.read.load(inputPath + inputFiles[2],
                                   format='com.databricks.spark.csv', 
                                   header='true', 
                                   inferSchema='true')


# Drop the first column  which is having Row number
df_normal_1 = df_normal_1.drop('_c0')
df_normal_2 = df_normal_2.drop('_c0')
df_bug_1 = df_bug_1.drop('_c0') 


# Displays the schema of the DataFrames
#df_normal_1.printSchema()
#df_normal_2.printSchema()
#df_bug_1.printSchema()

# Display the number of rows in the DataFrames
#print(df_normal_1.count())
#print(df_normal_2.count())
#print(df_bug_1.count())

# Display the number of columns in the DataFrames
#print(len(df_normal_1.columns))
#print(len(df_normal_2.columns))
#print(len(df_bug_1.columns))


# Create a list of all columns in all DataFrames without duplicates
merge_1_columns    = df_normal_1.columns  + [i for i in df_normal_2.columns if i not in df_normal_1.columns]
merged_columns    = merge_1_columns  + [i for i in df_bug_1.columns if i not in merge_1_columns]

# Display total number all columns
#print(len(merged_columns))

# Columns which are in "merged" but not in the corresponding DataFrame
columns_not_in_normal_1 = [i for i in merged_columns if i not in  df_normal_1.columns]
columns_not_in_normal_2 = [i for i in merged_columns if i not in  df_normal_2.columns]
columns_not_in_bug_1    = [i for i in merged_columns if i not in  df_bug_1.columns]


#print(len(columns_not_in_normal_1))
#print(len(columns_not_in_normal_2))
#print(len(columns_not_in_bug_1))


# Add columns which are missing
for new_column in columns_not_in_normal_1:
   df_normal_1       = df_normal_1.withColumn(new_column,lit(None).cast(NullType()))

for new_column in columns_not_in_normal_2:
   df_normal_2       = df_normal_2.withColumn(new_column,lit(None).cast(NullType()))

for new_column in columns_not_in_bug_1:
   df_bug_1          = df_bug_1.withColumn(new_column,lit(None).cast(NullType()))


# Change the order of columns to make all three uniform
df_normal_1 = df_normal_1.select(merged_columns)
df_normal_2 = df_normal_2.select(merged_columns)
df_bug_1    = df_bug_1.select(merged_columns)

# Display the number of columns in the DataFrames  after new columns are added
#print(len(df_normal_1.columns))
#print(len(df_normal_2.columns))
#print(len(df_bug_1.columns))


# Merge the DataFrames
df_merged  = df_normal_1.unionAll(df_normal_2.unionAll(df_bug_1))
print("Three Files are merged")
print("Three Files are merged")
print("Three Files are merged")
print("No. of Columns after merging : " + str(len(df_merged.columns)))
print("No. of Rows after merging :" + str(df_merged.count())) 

print("Imputation started")
# Performing Imputation
# Replace 'null' values with the  mean of that column
dict_mean_values = {}
#for column_name in merged_columns:
#    mean = df_merged.groupBy().avg(column_name).take(1)[0][0]
#    dict_mean_values[column_name] = mean

#df_merged = df_merged.fillna(dict_mean_values)

#df_merged.coalesce(1).write.option("header", "true").csv('merged')
print("Imputation completed")


#############################################################################################################


#  In case the stack execution is labeled as both normal and  anomalous, it is taken as a normal execution
#  i.e If  all columns except 'response' are same and if 'response' is '0' in one row and '1' in another ,  convert the '0' it into '1'

# list of column names without 'response'
#merged_columns_wo_response = list(merged_columns)
#merged_columns_wo_response.remove('response')
#merged_columns_wo_response_str = ""
#for col in merged_columns_wo_response:
#    merged_columns_wo_response_str = merged_columns_wo_response_str + "," + col

#merged_columns_wo_response_str  = merged_columns_wo_response_str.lstrip(',')

# Register the DataFrame as Table
#df_merged.registerTempTable("df_merged_table")


#x = sqlContext.sql("SELECT response,count(*) from df_merged_table GROUPBY " + merged_columns_wo_response_str + " HAVING count(*) > 1")

#x.show()

# list of column names without 'response'
#merged_columns_wo_response = list(merged_columns)
#merged_columns_wo_response.remove('response') 

#df_dup     = df_merged.groupBy(merged_columns_wo_response).count().filter('count > 1')
#df_non_dup = df_merged.groupBy(merged_columns_wo_response).count().filter('count = 1')

#print(df_dup.count())
#print(df_non_dup.count())

#df_dup_1     = df_merged.groupBy(merged_columns_wo_response).agg(collect_list('response').alias("response_2"),count('response').alias("count")).filter(col('count')>1)
#print(df_dup_1.count())
#df_dup_1.coalesce(1).write.option("header", "true").csv('duplicates3')
#df_dup_2     = df_dup_1.groupBy('response').count().filter('count>1')

#print("Duplicates are handled")


#################################################################################################################
# Logistic regression
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8,setLabelCol="reponse")

# Fit the model
#lrModel = lr.fit(df_merged)

# Print the coefficients and intercept for logistic regression
#print("Coefficients: " + str(lrModel.coefficients))
#print("Intercept: " + str(lrModel.intercept))
