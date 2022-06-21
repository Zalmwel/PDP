from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings (MRJob):
    def steps (self):
        return [
            MRStep(
                mapper=self.mapper_get_ratings,
                reducer=self.reducer_count_ratings,
            ),
            MRStep(    
                reducer= self.reducer_sorted_output
            )
            ]
    
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, movieID, values):
        yield sum(values), movieID

    def reducer_sorted_output(self, values, movieID):
        for id in movieID:
            yield id, values

if __name__ == '__main__':
    Ratings.run()
