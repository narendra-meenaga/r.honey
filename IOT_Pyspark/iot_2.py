#  SparkContext libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import NullType
from pyspark.ml.classification import LogisticRegression

import pyspark.mllib
import pyspark.mllib.regression
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel

# Create SparkContxt variable 'sc'
conf = SparkConf().setAppName("logistic regression on iot data")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Display only Errors
sc.setLogLevel("ERROR")

print("\n\n\n\n\n")



rdd = sc.textFile('Sacramentorealestatetransactions.csv')


#print(rdd.take(2))

rdd = rdd.map(lambda line: line.split(","))

#print(rdd.take(2))

header = rdd.first()
rdd = rdd.filter(lambda line:line != header)

#print(rdd.take(2))

df = rdd.map(lambda line: Row(street = line[0], city = line[1], zip=line[2], beds=line[4], baths=line[5], sqft=line[6], price=line[9])).toDF()

#print(df.take(2))

df.show(5)

df = df.select('price','baths','beds','sqft')


df_labelled = df.rdd.map(lambda line:LabeledPoint(line[0],[line[1:]]))
#print(df_labelled.take(5))


trainingData, testingData = df_labelled.randomSplit([.8,.2],seed=1234)

#Linear Regression
linearModel = LinearRegressionWithSGD.train(trainingData,10,.2)	

#print(linearModel.weights)

#print(testingData.take(5))

x  = linearModel.predict([1.0,1.0,760.0])

#print(format(x,'f'))


# Logistic regression

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("/home/ubuntu/spark-2.0.0-bin-hadoop2.7/data/mllib/sample_svm_data.txt")
parsedData = data.map(parsePoint)

# Build the model
model = LogisticRegressionWithLBFGS.train(parsedData)

#print(model.weights)

#print(parsedData.take(2))

#[LabeledPoint(1.0, [0.0,2.52078447202,0.0,0.0,0.0,2.00468443649,2.00034729927,0.0,2.22838704274,2.22838704274,0.0,0.0,0.0,0.0,0.0,0.0])


print(model.predict([0.0,2.52078447202,0.0,0.0,0.0,2.00468443649,2.00034729927,0.0,2.22838704274,2.22838704274,0.0,0.0,0.0,0.0,0.0,0.0]))

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
print(labelsAndPreds.take(2))
#trainErr = labelsAndPreds.map(lambda (v,p) : v+p) 
#print("Training Error = " + str(trainErr))

# Save and load model
#model.save(sc, "myModelPath")
#sameModel = LogisticRegressionModel.load(sc, "myModelPath")

#training = sqlContext.read.format("libsvm").load("/home/ubuntu/spark-2.0.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")
#print(type(training))

#lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
#lrModel = lr.fit(training)

# Print the coefficients and intercept for logistic regression
#print("Coefficients: " + str(lrModel.coefficients))
#print("Intercept: " + str(lrModel.intercept))
