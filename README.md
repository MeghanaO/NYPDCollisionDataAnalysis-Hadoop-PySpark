# NYPDCollisionDataAnalysis-Hadoop-PySpark
Hadoop Map Reduce and PySpark are used for the analysis of NYPD Collision Data

NYPD Collision Data Link: https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95

Using Map Reduce:

Command to run : yarn jar WordCount_Final.jar com.hadoop.partitioner.WithPartitioner /user/data/nypd/NYPD_Motor_Vehicle_WithOutHeader.txt /user/orugunma/output_meghana

Task1 : Cleaning of Data
	1. Input file location is passed as the first argument while running the jar
	2. Each line input in the mapper is split based on ',' delimiter but restricted to not split when it is in between ()
	3. Split values for the required 8 columns are checked for null values and non null rows are alone considered for task2
Task2 : Processing clean data
	1. Each key is uniquely delimited with a unique string and a tab delimiter
	2. Value for each key is calculated as accordingly like for the cases where maximum accidents needs to be taken then integer 1 is passed as value, for fatality the number of killed count is taken from persons, pedestrians, cyclists and motorits and also accordingly for the Year case as well.
	3. Intermediate Key values generated from mapper are passed to the reducer to create separate reducer tasks for each unique Key
	4. In reducer for each key the corresponding values are summed and the maximum value is taken and key is set.
	5. Once the each Key is processed, then final key values are written to the output folder specified as the second input argument while running the jar
Task3: Generating final Output file
	1. Above generated 8 part files can be merged in the same output folder location by using the command : hadoop fs -cat /user/orugunma/output_final/part*

For Spark:

Command to run :  spark-submit --master yarn SparkFinalCode.py "/user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt" "/user/orugunma/sparkResult"

Task1 : Cleaning of Data
	1. The input file is imported as a data frame 
	2. The column names are renamed as needed
	3. The other columns than the required 8 are removed
	4. Null values are dropped from the dataframe
Task2 : Processing clean data
	1. SQL queries for each required task is written by using the count function, group By and Order By function
	2. The resultant dataframes from each query are being joined using the union method of a dataframe
Task3: Generating final Output file
	1. By converting from dataframe to a RDD , the repartition method is used to create a single output file whose location is given in input argument 2
