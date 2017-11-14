# Pyspark script to perform logistic regression using MLLib on AWS
# Internet of Things Data

# Global settings 
inputPath  = "/home/ubuntu/iot_project/input_data/"
inputFiles = ['normal1.csv','normal2.csv','bug1.csv']

# Number of Iterations for Training the Model
numOfIterations = 10
# Fraction of the input data taken for Training  the Model . ex: 0.7 for 70%
trainingFraction = 0.9 
# Fraction of the input data taken for Testing the Model  
testingFraction = 0.1


#  SparkContext libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import  lit
from pyspark.sql.types import NullType
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from time import time

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
print("No. of Columns after merging : " + str(len(df_merged.columns)))
print("No. of Rows after merging :" + str(df_merged.count())) 

print("Imputation started")
# Performing Imputation
# Replace 'null' values with the  mean of that column
dict_mean_values = {}
for column_name in merged_columns:
    mean = df_merged.groupBy().avg(column_name).take(1)[0][0]
    dict_mean_values[column_name] = mean

df_merged = df_merged.fillna(dict_mean_values)


#df_merged.coalesce(1).write.option("header", "true").csv('merged')
print("Imputation completed")

# To bring 'response' to the beginning of the columns 
merged_columns.remove('response')
merged_columns.insert(0,'response')
df_merged  = df_merged.select(merged_columns)
#print(type(df_merged))
#df_merged.printSchema()

#############################################################################################################


#  In case the stack execution is labeled as both normal and  anomalous, it is taken as a normal execution
#  i.e If  all columns except 'response' are same and if 'response' is '0' in one row and '1' in another ,  convert the '0' it into '1'

###########################################################################################################

# Dimensionality Reduction

###########################################################################################################



# LogisticRegressionWithLBFGS

# Convert DataFrame to RDD to apply map
rdd_labelled_data = df_merged.rdd.map(lambda line:LabeledPoint(line[0],[line[1:]]))


# Split the data into training & testing data
training_data, testing_data = rdd_labelled_data.randomSplit([trainingFraction,testingFraction],seed=1234)

print("\n\nNumber of rows taken for Training : " +  str(training_data.count()))
print("Number of rows taken for Testing  : " +  str(testing_data.count()) + " \n\n")

t1 = time()

#print(training_data.take(2))
# Build the model
model = LogisticRegressionWithLBFGS.train(training_data,iterations=numOfIterations)

t2 = time() - t1

print("Time taken for Training : " + str(round(t2,2)) + " secs")

#print("Intercept of the Model : " + str(model.intercept))
#print("Weights of the Model : " + str(model.weights))



#########################################################################################################

t1 = time()

# Evaluating the model on testing data
labels_predictions = testing_data.map(lambda p: (p.label, model.predict(p.features[0])))
#print(type(labels_predictions))
#print(labels_predictions.take(3))
t2 = time() - t1

print("Time taken for Prediction : " + str(round(t2,2)) + " secs")


##########################################################################################################

# Accuracy = Total correct predictions /Total rows
accuracy = labels_predictions.filter(lambda  p: p[0] == p[1]).count()/ float(labels_predictions.count()) * 100
print("Accuracy of the Model : " + str(round(accuracy,2)) + "%")


# Precision = Total rows with response '1' and also predicted as '1' / (Total rows with response '1' and also predicted as '1' + Total rows with response '0' but predicted as '1'
true_positives  = labels_predictions.filter(lambda  p: p[0] == 1 & p[1] == 1).count()
false_positives = labels_predictions.filter(lambda  p: p[0] == 0 & p[1] == 1).count()

precision = (true_positives/(true_positives+false_positives))*100
print("Precision of the Model : " + str(round(precision,2)) + "%")


# Recall - Total rows with response '1' and also predicted as '1' / (Total rows with response '1' and also predicted as '1' + Total rows with response '1' but predicted as '0'
false_negatives = labels_predictions.filter(lambda  p: p[0] == 1 & p[1] == 0).count()

recall = (true_positives/(true_positives+false_negatives))*100
print("Recall of the Model : " + str(round(recall,2)) + "%")


##########################################################################################################

# Save the Model
#model.save(sc, "iotLRModel")
#sameModel = LogisticRegressionModel.load(sc, "iotLRModel")

