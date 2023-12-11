from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import sys

class JoinClientJob(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, value):
        itr = value.split()
        cwid = itr[0]
        yield cwid, value

    def reducer(self, key, values):
        student_rec = ""
        course_list = []

        for rec in values:
            itr = rec.split()
            cwid = itr[0]
            f_ind = itr[1]

            if f_ind.startswith("CPSC"):
                course_list.append(rec)
            else:
                student_rec = rec

        for c in course_list:
            value = f"{student_rec} {c}"
            yield key, value

if __name__ == '__main__':
    JoinClientJob.run()
