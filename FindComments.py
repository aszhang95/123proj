from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import spacy
import re
import numpy as np
#python3 taskx.py csvfile > newcsvfilename
nlp = spacy.load('en')

class MRFindComments(MRJob):
    '''

    ''' 
    def mapper_init(self):

        self.user_entity_dict = {}
        user_entity = open('find_comments.csv')
        user_entity = user_entity.readlines()
        self.num = 0

        for element in user_entity:
            temp = re.findall('\[(.*?)\]', element)
            match = re.findall('"(.*?)"', temp[0])
            user = match[0]
            self.user_entity_dict[user] = match[1]
        print(self.user_entity_dict)

    def mapper(self, _, line):

        user_comment = line.split(',')
        user = re.findall(r'"(.*)', user_comment[0])[0]
        comment = line[1]
        doc = nlp(comment)
        if user in self.user_entity_dict.values():
            if entity in [ent.text for ent in doc.ents]:
                yield user, comment
    def combiner(self, key, value):
        yield user, comment

    def reducer(self, user, count):

        yield user, comment

if __name__ == '__main__':
    MRFindComments.run()