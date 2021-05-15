from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Window

# Created spark session with local mode
spark = SparkSession.builder.master("local[*]").getOrCreate()

# path of the input file
path = "C:/Users/arpit/Desktop/EMR/input.csv"

# Reading the data using the csv option with header as true
df = spark.read.option("header",True).csv(path)

# Converting the timestamp column as timestamp
df = df.withColumn("Timestamp",f.col("timestamp").cast("timestamp"))

# Using the Window function to create partition within each users
win_spec = Window.partitionBy("userid").orderBy("Timestamp")

#Created a new column with time difference with one lagging timestamp 
df = df.withColumn("time_diff",(f.unix_timestamp(f.col("Timestamp"))-f.unix_timestamp(f.lag(f.col("Timestamp"),1).over(win_spec)))/60).na.fill(0)

# checking if the last activity timing was over if so the change increase the session by 1 otherwise 0
df = df.withColumn("temp_sess",f.when(f.col("time_diff")>=30,1).otherwise(0))

# Creating a cummulative session id for each change in the inactivity timing of 30 mins
df = df.withColumn("temp_sess_30_min",f.sum(f.col("temp_sess")).over(win_spec))

# now creating a second window to cater to the the need of checking the active time breaching the 2 hours limits
win_spec_2_hr = Window.partitionBy("userid","temp_sess_30_min").orderBy("Timestamp")

# creating it for summing all the previous fields time difference to keep a check on 2 hours limit
win_spec_2_hr_rows = win_spec_2_hr.rowsBetween(Window.unboundedPreceding,Window.currentRow)

# Created a column with time difference between two time intervals
df = df.withColumn("2_diff",(f.unix_timestamp(f.col("Timestamp"))-f.unix_timestamp(f.lag(f.col("Timestamp"),1).over(win_spec_2_hr)))/60).na.fill(0)

# here the actual check happens for each of the row where it was breaching the time interval or not
df = df.withColumn("time_diff_2_hr",f.when(f.sum(f.col("2_diff")).over(win_spec_2_hr_rows)>=120,1).otherwise(0))

# Here the final session is created by adding the 2 hours check and 30 mins cummulative check and adding 1 to it \
# and also by dropiing all redundent columns
df = df.withColumn("final_session_id",f.col("time_diff_2_hr")+f.col("temp_sess_30_min")+1).drop("time_diff", \
    "temp_sess","temp_sess_30_min","2_diff","time_diff_2_hr")

df.show()