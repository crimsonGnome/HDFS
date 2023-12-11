#performs a join operation between two datasets: the "Student" dataset and the "Course" dataset

from pyspark import SparkConf, SparkContext
import sys

def parse_student_line(line):
    fields = line.split(" ")
    cwid = fields[0]
    return cwid, fields[:]

def parse_course_line(line):
    fields = line.split(" ")
    cwid = fields[0]
    return cwid, fields[:]

def format_output(record):
    key, values = record
    student_info, course_info = values
    return "{}\t{} {}".format(key, " ".join(student_info), " ".join(course_info))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit your_script.py <student_file> <course_file> <output_directory>")
        sys.exit(-1)

    # Create the Spark application
    conf = SparkConf().setAppName("Outer Join Example")
    sc = SparkContext(conf=conf)

    # Loading the Student & Course datasets
    studentRDD = sc.textFile(sys.argv[2]).map(parse_student_line)
    courseRDD = sc.textFile(sys.argv[1]).map(parse_course_line)

    # Perform the Outer Join operation
    outerJoinRDD = studentRDD.fullOuterJoin(courseRDD)

    # Sort by key
    sortedRDD = outerJoinRDD.sortByKey()

    # Format the output
    formattedRDD = sortedRDD.map(format_output)

    # Save the result to a text file
    formattedRDD.saveAsTextFile(sys.argv[3])

    # Stop the Spark context
    sc.stop()

