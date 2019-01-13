import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
# Create a Spark Context
SpSession = SparkSession.builder.master("local[2]").appName("Spark App").config("spark.executor.memory","1g").getOrCreate()
# Create a dataframe with input imported from input argument 1
collisionData = SpSession.read.csv(sys.argv[1],header=True)
# Rename the column names with spaces inbetween them for using them in queries below
collisionData = collisionData.withColumnRenamed("#DATE","DATE").withColumnRenamed("ZIP CODE","ZIP").withColumnRenamed("NUMBER OF PERSONS INJURED","PERSONSINJURED").withColumnRenamed("NUMBER OF PERSONS KILLED","PERSONSKILLED").withColumnRenamed("NUMBER OF PEDESTRIANS INJURED","PEDESTRIANSINJURED").withColumnRenamed("NUMBER OF PEDESTRIANS KILLED","PEDESTRIANSKILLED").withColumnRenamed("NUMBER OF CYCLIST INJURED","CYCLISTINJURED").withColumnRenamed("NUMBER OF CYCLIST KILLED","CYCLISTKILLED").withColumnRenamed("NUMBER OF MOTORIST INJURED","MOTORISTINJURED").withColumnRenamed("NUMBER OF MOTORIST KILLED","MOTORISTKILLED").withColumnRenamed("VEHICLE TYPE CODE 1","VEHICLETYPE")
# Form list of the columns that needs to be removed
X = ["TIME","LATITUDE","LONGITUDE","LOCATION","ON STREET NAME","CROSS STREET NAME","OFF STREET NAME","CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2","CONTRIBUTING FACTOR VEHICLE 3","CONTRIBUTING FACTOR VEHICLE 4","CONTRIBUTING FACTOR VEHICLE 5","UNIQUE KEY","VEHICLE TYPE CODE 2","VEHICLE TYPE CODE 3","VEHICLE TYPE CODE 4","VEHICLE TYPE CODE 5"]
# Now the resultant dataframe holds only the required columns
collisionData = collisionData.drop(*X)
# Dropping the null values from the dataframe
collisionData = collisionData.dropna()
# Renaming the dataframe
collisionData.createOrReplaceTempView("collision")
# using spark sql the below queries give the required 8 sub tasks
query1 = SpSession.sql("SELECT  DATE AS KEY,COUNT(DATE) AS VALUE  FROM COLLISION GROUP BY DATE ORDER BY COUNT(DATE) DESC LIMIT 1")
query2 = SpSession.sql("SELECT  BOROUGH,SUM(PERSONSKILLED+PEDESTRIANSKILLED+CYCLISTKILLED+MOTORISTKILLED) FROM COLLISION GROUP BY BOROUGH ORDER BY SUM(PERSONSKILLED+PEDESTRIANSKILLED+CYCLISTKILLED+MOTORISTKILLED) DESC LIMIT 1")
query3 = SpSession.sql("SELECT  ZIP,SUM(PERSONSKILLED+PEDESTRIANSKILLED+CYCLISTKILLED+MOTORISTKILLED) FROM COLLISION GROUP BY ZIP ORDER BY SUM(PERSONSKILLED+PEDESTRIANSKILLED+CYCLISTKILLED+MOTORISTKILLED) DESC LIMIT 1")
query4 = SpSession.sql("SELECT  VEHICLETYPE ,COUNT(VEHICLETYPE)  FROM COLLISION GROUP BY VEHICLETYPE ORDER BY COUNT(VEHICLETYPE) DESC LIMIT 1")
query5 = SpSession.sql("SELECT  SUBSTR(DATE,7,10),SUM(PERSONSINJURED+PEDESTRIANSINJURED) FROM COLLISION GROUP BY SUBSTR(DATE,7,10) ORDER BY SUM(PERSONSINJURED+PEDESTRIANSINJURED) DESC LIMIT 1")
query6 = SpSession.sql("SELECT  SUBSTR(DATE,7,10),SUM(PERSONSKILLED+PEDESTRIANSKILLED) FROM COLLISION GROUP BY SUBSTR(DATE,7,10) ORDER BY SUM(PERSONSKILLED+PEDESTRIANSKILLED) DESC LIMIT 1")
query7 = SpSession.sql("SELECT  SUBSTR(DATE,7,10),SUM(CYCLISTINJURED+CYCLISTKILLED) FROM COLLISION GROUP BY SUBSTR(DATE,7,10) ORDER BY SUM(CYCLISTINJURED+CYCLISTKILLED) DESC LIMIT 1")
query8 = SpSession.sql("SELECT  SUBSTR(DATE,7,10),SUM(MOTORISTINJURED+MOTORISTKILLED) FROM COLLISION GROUP BY SUBSTR(DATE,7,10) ORDER BY SUM(MOTORISTINJURED+MOTORISTKILLED) DESC LIMIT 1")
# use union to combine the results of queries
m1 = query1.union(query2)
m2 = query3.union(query4)
m3 = query5.union(query6)
m4 = query7.union(query8)
r1 = m1.union(m2)
r2 = m3.union(m4)
final = r1.union(r2)
# Using repartition to create a single output file
rdd1 = final.rdd.repartition(1)
# The output folder location that holds the final result is given as input argument 2
rdd1.saveAsTextFile(sys.argv[2])
final.show()