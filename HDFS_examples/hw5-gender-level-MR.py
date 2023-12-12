from mrjob.job import MRJob
from mrjob.step import MRStep
from ast import literal_eval

class CalculateAvgGPA(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_gender_level,
                   reducer=self.reducer_identity),
            MRStep(mapper=self.mapper_format_output,
                   reducer=self.reducer_identity)
        ]

    def mapper_extract_gender_level(self, _, line):
        tokens = line.strip().split(" ")
        if tokens[1].startswith("CPSC"):
            return

        gender = tokens[3]
        level = int(tokens[4])
        yield (gender, level), line

    def reducer_identity(self, key, values):
        for value in values:
            yield key, value

    def mapper_format_output(self, key, value):
        yield key, f"{key} {value}"

if __name__ == '__main__':
    CalculateAvgGPA.run()
