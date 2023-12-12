# write a script to load this text file into an RDD and calculate the average salary by department.

from pyspark import SparkConf, SparkContext
import sys

def parse_people(line):
    fields = line.split(",")
    deptid = fields[2]
    salary = float(fields[3])
    return deptid, salary

def add_salary_count(salary1, salary2):
    total_salary = salary1[0] + salary2[0]
    total_count = salary1[1] + salary2[1]
    return total_salary, total_count

def calculate_average(total_salary_count):
    total_salary, total_count = total_salary_count
    return total_salary / total_count

if __name__ == "__main__":
    # Create the Spark application
    conf = SparkConf().setAppName("Average Salary by Department")
    sc = SparkContext(conf=conf)

    peopleRDD = sc.textFile(sys.argv[1]).map(parse_people)

    # Create an RDD with (department, (salary, 1))
    dept_salary_count = peopleRDD.mapValues(lambda salary: (salary, 1))

    # Reduce by key to calculate total salary and count of employees per department
    dept_salary_total = dept_salary_count.reduceByKey(add_salary_count)

    # Calculate average salary per department
    avg_salary_per_dept = dept_salary_total.mapValues(calculate_average)

    # Save the result to a text file
    avg_salary_per_dept.saveAsTextFile(sys.argv[2])

    # Stop the Spark context
    sc.stop()
