from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("StudentID", StringType(), True),
    StructField("CourseCode", StringType(), True),
    StructField("Grade", FloatType(), True),
])

# Read data from a text file into a DataFrame
file_path = "/home/crimsongnome/Desktop/HDFS_Map_Reduce/HDFS_examples"
df = spark.read \
    .text(file_path) \
    .rdd \
    .map(lambda x: x[0].split()) \
    .toDF(schema=schema)

# Show the DataFrame
df.show()