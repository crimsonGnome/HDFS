# calculate the average grade point average (GPA) for each student based on Course.txt

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
        total_gpa = 0.0
        num_courses = 0

        for value in values:
            tokens = value.strip().split()
            input_type = tokens[1]

            if input_type.startswith("CPSC"):
                grade = float(tokens[2])
                total_gpa += grade
                num_courses += 1
            else:
                student_rec = value

        average_gpa = total_gpa / num_courses if num_courses > 0 else 0.0

        value = f"Average GPA: {average_gpa:.2f}"
        yield key, value


if __name__ == '__main__':
    JoinMRJob.run()
