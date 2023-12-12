# Write a MapReduce program to calculate the total sales for each product.
# Total sales are calculated as the sum of Price * Quantity for each product.

from mrjob.job import MRJob
from mrjob.step import MRStep


class SalesByProduct(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper)
        ]

    def mapper(self, _, line):
        fields = line.strip().split(" ")
        if len(fields) >= 4:
            product_id = fields[1]
            price = float(fields[2])
            quantity = int(fields[3])
            revenue = price * quantity
            yield product_id, revenue

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    SalesByProduct.run()