# pyspark_job.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
def create_spark_session():
    """Create spark session.
Returns:
        spark (SparkSession) - spark session connected to AWS EMR
            cluster
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
def process_emp_data(spark, input_path, output_path):
    """Process the Employee data spread accross different folders and write to S3 as 5 different csv files.
        Arguments:
            spark (SparkSession) - spark session connected to AWS EMR
                cluster
            input_path (str) - AWS S3 bucket path for source data
            output_path (str) - AWS S3 bucket for writing processed data
    """
    input_path_schema=input_path+'/emp*with_schema.csv'
    input_path_without_schema=input_path+'/emp*without_schema.csv'
    df = spark.read.options(header='True', delimiter=',').csv(input_path_schema)
    df_without_schema = spark.read.options(header='False',delimiter=",").csv(input_path_without_schema)
    # Apply some basic filters and aggregate by department_code.
	
    # Save the data to your S3 bucket as a .csv file.
    df_union = df.union(df_without_schema)
    df_union.repartition("department_code").write.option("header",True).partitionBy("department_code").mode('overwrite').csv(output_path)
	
def main():
    spark = create_spark_session()
    input_path = ('s3://emr-cluster-bootstrap1/emp*' +
                  '/emp*with_schema.csv')
    output_path = 's3://emr-cluster-output-emp'
    process_emp_data(spark, input_path, output_path)
if __name__ == '__main__':
    main()