#Perform joins on student.txt and course.txt
#Perform filter on male and female students
from mrjob.job import MRJob
from mrjob.step import MRStep

class FilterGender(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_join,
                   reducer=self.reducer_join),
            MRStep(mapper=self.mapper_filter_gender,
                   reducer=self.reducer_filter_gender),
            MRStep(mapper=self.mapper_gpa,
                   reducer=self.reducer_gpa_count),
            MRStep(reducer=self.reducer_avg_gpa)
        ]

    def mapper_join(self, _, line):
        fields = line.strip().split(" ")
        key = fields[0]

        if fields[1].startswith("CPSC"):
            data_type = "course"
        else:
            data_type = "student"

        yield key, (data_type, line)

    def reducer_join(self, key, values):
        student_info = None
        course_infos = []

        for data_type, data in values:
            if data_type == 'course':
                course_infos.append(data)
            else:
                student_info = data

        # Output each course info along with the student info
        for course_info in course_infos:
            yield key, (student_info, course_info)

    def mapper_filter_gender(self, key, line):
        student_info, course_info = line

        if len(student_info)>3:
            gender = student_info.strip().split(" ")[3].lower()
            if gender != 'male':
                yield key, course_info

    def reducer_filter_gender(self, key, values):
        for line in values:
            yield key, line

    def mapper_gpa(self, key, line):
        course_info = line
        grade = float(course_info.strip().split(" ")[2])
        yield key, (grade, 1)

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
    FilterGender.run()
