# Use a filter to select only reviews with a score above 3, and then apply mapToPair
# to create a pair RDD with ProductID as the key and ReviewText as the value.
from pyspark import SparkConf, SparkContext
import sys

def parse_reviews(line):
    fields = line.split(",")
    revid = fields[0]
    review_score = float(fields[1])
    review_text = fields[2:]
    return (revid, review_score, review_text)

def filter_good_reviews(record):
    _, score, _ = record
    return score >= 3

def create_pair(record):
    revid, _, review_text = record
    return (revid, review_text)

if __name__ == "__main__":
    conf = SparkConf().setAppName("Review Analysis")
    sc = SparkContext(conf=conf)

    # Read and parse the data
    reviewsRDD = sc.textFile(sys.argv[1]).map(parse_reviews)

    # Filter for reviews with a score of 3 or higher
    goodReviewsRDD = reviewsRDD.filter(filter_good_reviews)

    # Create a pair RDD with ProductID as the key and ReviewText as the value
    pairedRDD = goodReviewsRDD.map(create_pair)

    # Save the results
    pairedRDD.saveAsTextFile(sys.argv[2])

    sc.stop()

