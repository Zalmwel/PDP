from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings(MRJob):
    def steps(self):
        return [
        MRStep(
            mapper=self.mapper_get_ratings,
            combiner=self.combiner_count_ratings,
            reducer=self.reducer_count_ratings),
        MRStep(
            reducer=self.reducer_sorted_output
        )]


    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def combiner_count_ratings(self, movieID, counts):
        yield movieID, sum(counts)

    def reducer_count_ratings(self, movieID, counts):
        yield None, (sum(counts), movieID)

    def reducer_sorted_output(self, _, values):
        for counted_ratings, movieID in sorted(values, reverse=True):
            yield(int(movieID), counted_ratings)

if __name__ == '__main__':
    Ratings.run()