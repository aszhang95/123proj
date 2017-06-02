from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import spacy
import re
import numpy as np
#python3 taskx.py csvfile > newcsvfilename


class MRMakeSentences(MRJob):
    '''

    ''' 
    def mapper_init(self):

        self.active_user_list = np.zeros(100000, dtype=object)
        active = open('active.csv')
        active = active.readlines()
        self.num = 0

        for ind, user in enumerate(active):
            user = user.strip()
            user_edit = re.findall(r'"\\"(.*)"', user)
            self.active_user_list[ind] = user_edit[0]
        print('here!')

        #print(self.active_user_list)

    def mapper(self, _, line):


        parsed = line.split(',')
        user = re.findall(r'"(.*)',parsed[0])[0]

        if user in self.active_user_list:
            #print(line)
            comment = re.sub("[^(\w\s)]", "", parsed[1])
            comment = comment.strip()
            yield (user, comment), None

    def reducer(self, user, count):

        yield user, None

    def steps(self):

        return [
          MRStep(
                 mapper_init=self.mapper_init,
                 mapper=self.mapper,
                 reducer=self.reducer)]

if __name__ == '__main__':
    MRMakeSentences.run()