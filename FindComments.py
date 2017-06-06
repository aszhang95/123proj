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
        #initialize list of users from CSVofUsers.csv
        self.user_list = []
        users = open('CSVofUsers.csv')
        users = users.readlines()

        for user in users:
            user_list.append(user)

    def mapper(self, _, line):
        #get/clean user and comment from data/small_new_data.csv
        user_comment = line.split(',')
        user_comment = str(user_comment)
        user_comment.strip()[1:len(user_comment)-1]
        user_comment.strip()[1:len(user_comment)-1].split(",")
        temp = re.findall('\[(.*?)\]', user_comment)
        match = re.findall('"(.*?)"', temp[0])  
        comment = match[1]
        user = match[0]

        #check if user is in self.user_list
        #if so, yield user and comment
        doc = nlp(comment)
        if user in self.user_list:
            yield user, comment
    def combiner(self, user, comment):
        yield user, comment

    def reducer(self, user, comment):
        print((user,comment))
        yield user, comment

if __name__ == '__main__':
    MRFindComments.run()