# Using PySpark, write a script to load this CSV into a DataFrame and select rows where "Age" is less than 25.
# Utilize the filter or where method to apply this narrow transformation.

from pyspark import SparkConf, SparkContext
import sys

def parse_people(line):
    fields = line.split(",")
    id = fields[0]
    return id, fields

def is_young(line):
    fields = line[1]  # Access the fields from the parsed line
    id = line[0]  # Access the ID
    age = int(fields[2])
    if age < 25:  # Change the condition to keep people younger than 25
        return id, fields

if __name__ == "__main__":

    # Create the Spark application
    conf = SparkConf().setAppName("Filter Example")
    sc = SparkContext(conf=conf)

    peopleRDD = sc.textFile(sys.argv[1]).map(parse_people)

    youngRDD = peopleRDD.filter(is_young)

    # Save the result to a text file
    youngRDD.saveAsTextFile(sys.argv[2])

    # Stop the Spark context
    sc.stop()

