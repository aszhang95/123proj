from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import numpy as np
import ast
import re


class MRUserbyUserMatrix(MRJob):
    '''
    Takes the csv with sentence pairs and creates a csv that gives the 
    location of the matrix and the similarity score that should be put there

    Input:
        csv file with each line like --> sentence 1, user of sentence 1, sentence 2, user of sentence 2

    Output:
        csv file with each line like --> (x coosrdinate of matrix, y coordinate of matrix), similarity score
    '''

    def mapper_init(self):

        self.current_dict_location = 0
        self.user_dict = {}

    def mapper(self, _, pair):

        pair_info = [x.strip() for x in pair.split(',')]
        sentence1 = re.findall(r'\'(.*?)\'', pair_info[1])
        sentence1_user = pair_info[0]
        sentence1_user = re.findall(r'\'(.*?)\'', pair_info[0])[0]
        sentence1_vector = ",".join(pair_info[2:302])
        sentence1_vector = '[' + sentence1_vector + ']'
        sentence1_vector = np.asarray(ast.literal_eval(sentence1_vector))
        sentence2 = re.findall(r'\'(.*?)\'', pair_info[303])
        sentence2_user = re.findall(r'\'(.*?)\'', pair_info[302])[0]
        sentence2_vector = ",".join(pair_info[304:])
        sentence2_vector = '[' + sentence2_vector.strip('"')
        sentence2_vector = np.asarray(ast.literal_eval(sentence2_vector))
        if sentence1_user not in self.user_dict:
            self.user_dict[sentence1_user] = self.current_dict_location
            self.current_dict_location += 1
        if sentence2_user not in self.user_dict:
            self.user_dict[sentence2_user] = self.current_dict_location
            self.current_dict_location += 1

        similarity_score = np.dot(sentence1_vector,sentence2_vector)

        yield (sentence1_user, self.user_dict[sentence1_user], sentence2_user,self.user_dict[sentence2_user]), similarity_score
        yield (sentence2_user, self.user_dict[sentence2_user], sentence1_user, self.user_dict[sentence1_user]), similarity_score        

    def combiner(self, users, scores):

        yield users, sum(scores)
    
    def reducer_init(self):

        active = open('active.csv')
        active = active.readlines()

        self.num_comments_dict = {}

        for line in active:
            user = re.findall(r'"(.*)"', line)[0]
            num_comments = ast.literal_eval(re.findall(r'\t(\d+)', line)[0])
            self.num_comments_dict[user] = num_comments


    def reducer(self, users, scores):

        normal_factor = self.num_comments_dict[users[0]] * self.num_comments_dict[users[2]]
        yield (users), sum(scores)/ normal_factor

    def steps(self):

        return [
          MRStep(mapper_init=self.mapper_init,
                 mapper=self.mapper,
                 combiner=self.combiner,
                 reducer_init = self.reducer_init,
                 reducer=self.reducer)]

if __name__ == '__main__':
    MRUserbyUserMatrix.run()
