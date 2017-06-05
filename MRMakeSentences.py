from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import spacy
import re
import numpy as np
#python3 taskx.py csvfile > newcsvfilename
nlp = spacy.load('en')

class MRMakeSentences(MRJob):
    '''

    ''' 
    def mapper_init(self):

        self.active_user_list = np.zeros(200, dtype=object)
        active = open('active.csv')
        active = active.readlines()

        for ind, user in enumerate(active):
            user = user.strip()
            user_edit = re.findall(r'"(.*)"', user)
            self.active_user_list[ind] = user_edit[0]

        #print(len(self.active_user_list))

    def mapper(self, _, line):


        parsed = line.split(',')
        user = re.findall(r'"(.*)',parsed[0])[0]

        if user in self.active_user_list:
            #print(line)
            comment = re.sub("[^(\w\s)]", "", parsed[1])
            comment = comment.strip()
            doc = nlp(comment)
            yield ((user, str(doc), str(doc.vector)), None)

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