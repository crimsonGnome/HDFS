#Write a MapReduce program to filter out lines that contain a specific keyword, say "the". Your program should output only the lines that do not contain this keyword.

from mrjob.job import MRJob
from mrjob.step import MRStep


class RemoveThe(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper)
        ]

    def mapper(self, _, line):
        words = line.split()

        # Filter out words that are equal to 'the' (case-insensitive)
        filtered_words = []
        for word in words:
            if word.lower() != 'the':
                filtered_words.append(word)

        # Join the filtered words back into a line
        new_line = ' '.join(filtered_words)

        yield _, new_line


if __name__ == '__main__':
    RemoveThe.run()

