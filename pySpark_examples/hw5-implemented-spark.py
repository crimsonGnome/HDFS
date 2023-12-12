#Perform joins on departRDD, studentRDD and courseRDD. Perform groupby and aggregate operation on the above join result.

#SELECT * FROM student JOIN course ON student.cwid = course.cwid JOIN dept ON student.departmentID = dept.deptid;

import sys

from pyspark import SparkConf, SparkContext

import sys
from pyspark import SparkConf, SparkContext

def map_student(s):
    # Extracts CWID and the rest of the information
    fields = s.split(" ")
    cwid = fields[0]
    return cwid, s

def map_course(s):
    # Extracts CWID and the rest of the information
    fields = s.split(" ")
    cwid = fields[0]
    return cwid, s


def map_dept(s):
    # Extracts Department ID and the rest of the information
    fields = s.split(" ")
    dept_id = fields[0]
    return dept_id, s


if __name__ == "__main__":
    # Create the Spark application
    spark_conf = SparkConf().setAppName("Join Course, Student, and Dept").setMaster("local[*]")

    # Create the application object
    sc = SparkContext(conf=spark_conf)

    # Loading the Course, Student, and Dept datasets
    course_rdd = sc.textFile(sys.argv[1])
    student_rdd = sc.textFile(sys.argv[2])
    dept_rdd = sc.textFile(sys.argv[3])

    # Map to PairRDD
    student_pair_rdd = student_rdd.map(map_student)
    course_pair_rdd = course_rdd.map(map_course)
    dept_pair_rdd = dept_rdd.map(map_dept)

    # Perform the Join operations
    join_student_course_rdd = student_pair_rdd.join(course_pair_rdd)

    def extract_dept_id(record):
        (_, (student_info, _)) = record
        dept_id = student_info.split(" ")[-1]  # Assuming deptID is at the end of the string
        return dept_id, record

    dept_id_keyed_rdd = join_student_course_rdd.map(extract_dept_id)

    join_result_rdd = dept_id_keyed_rdd.join(dept_pair_rdd)
    join_result_rdd.saveAsTextFile(sys.argv[4])

    # # Extract the desired fields from the join result
    # def extract_fields(record):
    #     (_, ((student_info, course_info), dept_info)) = record
    #     return f"{student_info} {course_info} {dept_info}"
    #
    # result_rdd = join_result_rdd.map(extract_fields)
    #
    # # Save the result to a text file
    # result_rdd.saveAsTextFile(sys.argv[4])
    #
    # # Print the result
    # for record in result_rdd.collect():
    #     print(record)

    # Stop the Spark context
    sc.stop()
