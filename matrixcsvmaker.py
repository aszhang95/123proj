from mrjob.job import MRJob
from mrjob.step import MRStep
import spacy
import sys
import csv
import numpy as np
from functools import reduce
import operator

nlp = spacy.load('en')

class MRUserbyUserMatrix(MRJob):
    '''
    Takes the csv with sentence pairs and creates a csv that gives the 
    location of the matrix and the similarity score that should be put there

    Input:
        csv file with each line like --> sentence 1, user of sentence 1, sentence 2, user of sentence 2

    Output:
        csv file with each line like --> (x coordinate of matrix, y coordinate of matrix), similarity score
    '''

    def mapper_init(self):

        current_dict_location = 0
        user_dict = {}

    def mapper(self, _, pair):

        pair_info = [x.strip() for x in pair.split(',')]
        sentence1 = pair_info[0]
        sentence1_user = pair_info[1]
        sentence2 = pair_info[2]
        sentence2_user = pair_info[3]

        if sentence1_user not in self.user_dict:
            self.user_dict[sentence1_user] = current_dict_location
            current_dict_location += 1
        if sentence2_user not in self.user_dict:
            self.user_dict[sentence2_user] = current_dict_location
            current_dict_location += 1

        sentence1nlp = nlp(sentence1)
        sentence2nlp = nlp(sentence2)

        similiarity_score = sentiment_calculator(sentence1nlp, sentence2nlp)

        yield (sentence1_user, sentence2_user), similarity_score
        yield (sentence2_user, sentence1_user), similarity_score        

    def combiner(self, users, scores):

        yield users, reduce(operator.mul, (scores), 1)
    
    def reducer(self, user, scores):

        user_row_index = self.user_dict[user[0]]
        user_col_index = self.user_dict[user[1]]
        yield (self.user_dict[user[0]], self.user_dict[user[1]]), reduce(operator.mul, (scores), 1)

    def steps(self):

        return [
          MRStep(mapper_init=self.mapper_init,
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
                 reducer=self.inactive_user_reducer)]

if __name__ == '__main__':
    MRUserbyUserMatrix.run()
