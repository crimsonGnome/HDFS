from mrjob.job import MRJob
from mrjob.step import MRStep

class ACClientJob(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        fields = line.split(" ")
        if len(fields) >= 4:
            academic_level = fields[-1]
            yield academic_level, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    ACClientJob.run()
