from mrjob.job import MRJob

class MyMRJob(MRJob):
    def mapper(self, _, line):
        line = line.strip()
        words = line.split()
        for word in words:
            yield (word, 1)

    def reducer(self, key, list_of_values):
        word = key
        total_count = sum(list_of_values)
        yield None, (total_count, word)

    def reducer2(self, _, list_of_values):
        N = 3
        list_of_values = sorted(list(list_of_values), reverse=True)
        return list_of_values[:N]
    def steps(self):
        return [self.mr(mapper=self.mapper,
        reducer=self.reducer), self.mr(reducer=self.reducer2)]
        if __name__ == ‘__ma