from pyspark import SparkContext
from pyspark.serializers import MarshalSerializer
import sys
import csv

inputfile = sys.argv[1]
outputfile = sys.argv[2]

sc = SparkContext('local','task1',serializer = MarshalSerializer())

infile = sc.textFile(inputfile)

m1 = infile.map( lambda x: x.split(',') )                                   #split lines
m2 = m1.filter(lambda x: 'userId' not in x )                                #remove headers
m3 = m2.map( lambda x: (  int(x[1]) , (float(x[2]) , 1.0) ) )               #store key -> (value,count)
m4 = m3.reduceByKey( lambda x, y: (x[0]+y[0],x[1]+y[1])  )                  # reduce to key -> (total value, total count)

#final output
m5 = m4.map(lambda x: (x[0],(x[1][0]/x[1][1]) )).sortByKey(ascending=True).collect()  #map x - > (x , avgrating(x))

with open(outputfile, 'wb') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["movieId","rating_avg"])
    for key,value in m5:
       writer.writerow([key, value])
