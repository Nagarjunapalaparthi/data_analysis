import matplotlib.pyplot as plt
from pyspark.sql import *
from pyspark import SparkContext,SparkConf

sc = SparkContext(conf=SparkConf())
sqlContext = SQLContext(sc)
var1 = sc.textFile("retail.csv")
data = var1.map(lambda x:x.split(','))
df = sqlContext.createDataFrame(data)
df.createOrReplaceTempView("table1")


reg = sqlContext.sql("select * from table1 where _11='Regular Air'")
exp = sqlContext.sql("select * from table1 where _11='Express Air'")
dev = sqlContext.sql("select * from table1 where _11='Delivery Truck'")
list = [reg.count(),exp.count(),dev.count()]

print "Regular air deliveries:",list[0]
print "Express air deliveries:",list[1]
print "Truck deliveries:",list[2]

labels = ['Regular','Express','Truck']


plt.pie(list,labels=labels)
plt.show()

sc.stop()

~                                                                                                                                           
~                                                                                                                                           
~                
