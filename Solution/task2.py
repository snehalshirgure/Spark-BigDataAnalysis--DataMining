from pyspark import SparkContext
from pyspark.serializers import MarshalSerializer
from pyspark.sql import SQLContext

import sys
import csv

inputfile = sys.argv[1]
inputfile2 = sys.argv[2]
outputfile = sys.argv[3]

sc = SparkContext('local','task1',serializer = MarshalSerializer())
infile1 = sc.textFile(inputfile)
infile2 = sc.textFile(inputfile2)


m1 = infile1.map( lambda x: x.split(',') ).filter(lambda x: 'userId' not in x ).map( lambda x: (  int(x[1]) , (float(x[2])) ) )
n1 = infile2.map( lambda x: x.split(',') ).filter(lambda x: 'userId' not in x ).map( lambda x: (  int(x[1]) , (x[2]) ) )

m2 = n1.leftOuterJoin(m1).filter(lambda x: x[1][1]!=None).map(lambda x: ( x[1][0] , (x[1][1],1.0))).reduceByKey(lambda x,y: (x[0]+y[0] , x[1]+y[1]) )
m3 = m2.map(lambda x: (x[0],(x[1][0]/x[1][1]) )).sortByKey(ascending=False).collect()

#print(m3.collect())

#sqlContext = SQLContext(sc)
#df = sqlContext.createDataFrame(m3, ['tag', 'rating_avg'])
#df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').csv(outputfile)

with open(outputfile, 'wb') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["tag","rating_avg"])
    for key,value in m3:
       writer.writerow([unicode(key).encode("utf-8"), value]) 

#m3.saveAsTextFile(outputfile)                                   