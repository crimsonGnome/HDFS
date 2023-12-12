# Create pair RDDs from both files, with CustomerID as the key.
# Perform an inner join to associate each transaction with the customer's name.
from pyspark import SparkConf, SparkContext
import sys

def parse_cust(line):
    fields = line.split(",")
    id = fields[0]
    info = fields[1:]
    return id, info

def parse_transaction(line):
    fields = line.split(",")
    id = fields[1]
    info = [fields[0], fields[2]]
    return id, info

if __name__ == "__main__":
    conf = SparkConf().setAppName("Review Analysis")
    sc = SparkContext(conf=conf)

    # Read and parse the data
    custRDD = sc.textFile(sys.argv[1]).map(parse_cust)
    transRDD = sc.textFile(sys.argv[2]).map(parse_transaction)

    joinedRDD = custRDD.join(transRDD)

    # Save the results
    joinedRDD.saveAsTextFile(sys.argv[3])

    sc.stop()

