#SELECT academicLevel, COUNT(*) FROM Student GROUP BY academicLevel

from pyspark import SparkConf, SparkContext
import sys

def parse_student_line(line):
    fields = line.split(" ")
    cwid, academic_level = fields[0], fields[4]
    return academic_level, 1

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: spark-submit your_script.py <student_file> <output_directory>")
    #     sys.exit(-1)

    # Create the Spark application
    conf = SparkConf().setAppName("Group By Example")
    sc = SparkContext(conf=conf)

    # Loading the Student dataset
    studentRDD = sc.textFile(sys.argv[1]).map(parse_student_line)

    # Step 1: Group by academic level
    groupedByKey = studentRDD.groupByKey()

    # Step 2: Count the occurrences of each academic level
    groupedRDD = groupedByKey.mapValues(len)

    sortedRDD = groupedRDD.sortByKey(ascending=True)

    # Save the result to a text file
    groupedRDD.saveAsTextFile(sys.argv[2])

    # Stop the Spark context
    sc.stop()
