#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep


class JoinMRJob(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Split the input line into tokens
        tokens = line.strip().split()
        cwid = tokens[0]
        yield cwid, line

    def reducer(self, key, values):
        student_rec = ""
        course_list = []

        for value in values:
            tokens = value.strip().split()
            cwid = tokens[0]
            f_ind = tokens[1]

            if f_ind.startswith("CPSC"):
                course_list.append((tokens[1], tokens[2]))
            else:
                student_rec = value

        for course in course_list:
            value = f"{student_rec[:]} {course[0]} {course[1]}"
            yield key, value


if __name__ == '__main__':
    JoinMRJob.run()
