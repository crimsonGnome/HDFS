from pyspark.sql import SparkSession

def word_count(input_file, output_file):
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Read input text file
    lines = spark.read.text(input_file).rdd.map(lambda r: r[0])

    # Split each line into words and assign count of 1 to each word
    words = lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))

    # Reduce by key (word) and sum the counts
    word_counts = words.reduceByKey(lambda x, y: x + y)

    # Save the result to the output file
    word_counts.saveAsTextFile(output_file)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    input_path = "hdfs:///path/to/your/input/file.txt"
    output_path = "hdfs:///path/to/your/output/directory"

    word_count(input_path, output_path)