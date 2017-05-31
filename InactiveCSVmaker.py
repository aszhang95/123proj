from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import json
#python3 taskx.py csvfile > newcsvfilename

class MRFindInactiveUsers(MRJob):
    '''
    This will yield the inactive users (less than 5 comments) into a csv file
    as user, None
    ''' 

    def inactive_user_mapper(self, _, data):

        data = json.loads(line)
        user = data["author"]
        if user != "[deleted]":
            comment = data["body"]
            comment = comment.strip()
            yield user, 1
    
    def inactive_user_combiner(self, user, count):

        yield user, sum(count)

    def inactive_user_reducer(self, user, count):

        if sum(count) < 3:
            yield (user, None)

    def steps(self):

        return [
          MRStep(
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
                 reducer=self.inactive_user_reducer)]

if __name__ == '__main__':
    MRFindInactiveUsers.run()


