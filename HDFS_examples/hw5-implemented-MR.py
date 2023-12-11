#Perform joins on department.txt, student.txt and course.txt
#Perform groupby and aggregate operation on the above join result.

from mrjob.job import MRJob
from mrjob.step import MRStep

class CalculateAvgGPA(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_join,
                   reducer=self.reducer_join),
            MRStep(mapper=self.mapper_gpa,
                   reducer=self.reducer_gpa_count),
            MRStep(reducer=self.reducer_avg_gpa)
        ]

    def mapper_join(self, _, line):
        fields = line.strip().split(" ")
        key = fields[0]

        if fields[1].startswith("CPSC"):
            data_type = "course"
        elif len(fields) < 6:
            data_type = "dept"
        else:
            data_type = "student"

        yield key, (data_type, line)

    def reducer_join(self, key, values):
        student_dept = None
        course_info = None

        for data_type, data in values:
            if data_type == 'student':
                student_dept = data.split()[-1]
            elif data_type == 'course':
                course_info = data

        yield key, (student_dept, course_info,)

    def mapper_gpa(self, _, line):
        student_dept, course_info = line
        grade = 0.0
        if student_dept is not None:
            student_dept = student_dept.split(" ")[0]
        if course_info is not None:
            grade = float(course_info.split(" ")[2])
        yield student_dept, (grade, 1)

    def reducer_gpa_count(self, key, values):
        total_gpa = 0.0
        count = 0

        for grade, one in values:
            total_gpa += grade
            count += one

        yield key, (total_gpa, count)

    def reducer_avg_gpa(self, key, values):
        total_gpa = 0.0
        total_count = 0

        for gpa, count in values:
            total_gpa += gpa
            total_count += count

        avg_gpa = total_gpa / total_count if total_count > 0 else 0.0

        yield key, avg_gpa

if __name__ == '__main__':
    CalculateAvgGPA.run()
