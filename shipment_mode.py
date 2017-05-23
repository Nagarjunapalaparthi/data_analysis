import matplotlib.pyplot as plt
from pyspark.sql import *
from pyspark import SparkContext,SparkConf

sc = SparkContext(conf=SparkConf())
sqlContext = SQLContext(sc)
rdd = sc.textFile("retail.csv")
data = rdd.map(lambda x:x.split(','))
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
cols = ("blue","yellow","orange")


plt.pie(list,labels=labels,colors=cols,startangle=90,shadow=True,explode=(0.2,0,0),autopct=("%1.1f%%"))
plt.show()

sc.stop()

~                                                                                                                                           
~                                                                                                                                           
~                
