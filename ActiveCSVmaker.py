from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import json
#python3 taskx.py csvfile > newcsvfilename

class MRFindActiveUsers(MRJob):
    '''
    This will yield the active users (more than 20 comments) into a csv file
    as user, None
    ''' 

    def active_user_mapper(self, _, line):

        data = json.loads(line)
        user = data["author"]
        if user != "[deleted]":
            comment = data["body"]
            comment = comment.strip()
            yield user, 1
    
    def active_user_combiner(self, user, count):

        yield user, sum(count)

    def active_user_reducer(self, user, count):

        yield (user, None)

    def steps(self):

        return [
          MRStep(
                 mapper=self.active_user_mapper,
                 combiner=self.active_user_combiner,
                 reducer=self.active_user_reducer)]

if __name__ == '__main__':
    MRFindActiveUsers.run()


