from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import json
import spacy
import re
#python3 taskx.py csvfile > newcsvfilename
nlp = spacy.load('en')

class MRMakeSentences(MRJob):
    '''

    ''' 
    def mapper_init(self):

        self.active_user_list = []
        active = open('active.csv')
        active = active.readlines()

        for user in active:
            user = user.strip()
            user_edit = re.findall(r'"(.*?)"', user)
            self.active_user_list.append(user_edit[0])

    def mapper(self, _, line):

        data = json.loads(line)
        user = data["author"]
        if user != "[deleted]":
            if user in self.active_user_list:
                comment = data["body"]
                comment = comment.strip()
                doc = nlp(comment)
                yield (user, doc.vector), None

    def reducer(self, user, count):

        yield user, None

    def steps(self):

        return [
          MRStep(
                 mapper_init=self.mapper_init,
                 mapper=self.mapper,
                 combiner=self.combiner,
                 reducer=self.reducer)]

if __name__ == '__main__':
    MRMakeSentences.run()