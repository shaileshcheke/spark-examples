from pyspark import SparkContext,SparkConf
sc=SparkContext()
rddD=sc.textFile("hdfs://localhost:54310/data/hadoop/spark-input/wc.txt")
rddS=sc.textFile("hdfs://localhost:54310/data/hadoop/spark-input/stopwords_en.txt")
flM=rddD.flatMap(lambda x: x.split()).map(lambda x: (x.lower(),1)).reduceByKey(lambda x,y:x+y)
flS=rddS.flatMap(lambda x: x.split("\n")).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)
for i in flS.collect():
	print i
flM=flM.subtractByKey(flS)
def comp(x,y):
	if x[1]<y[1]:
		return y
	else:
		return x

maxOcc=flM.reduce(comp)
print maxOcc
