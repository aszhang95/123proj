from mrjob.job import MRJob
from mrjob.step import MRStep
import spacy
import sys
import csv
import numpy as np
from functools import reduce
import operator

nlp = spacy.load('en')

class MRFindInactiveUsers(MRJob):

    def inactive_user_mapper(self, _, comment):

        comment_info = [x.strip() for x in comment_info.split(',')]
        comment = ''.join(comment_info[:-20])
        username = comment_info[-17]
        if username != '[deleted]':
            yield username, 1
    
    def inactive_user_combiner(self, user, count):

        yield user, sum(count)

    def reducer_init(self):

        self.inactive_user_list = []

    def inactive_user_reducer(self, user, count):

        if sum(count) < 5:
            self.inactive_user_list.append(user)

    def reducer_final(self):

        yield self.inactive_user_list, None

    def steps(self):

        return [
          MRStep(
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
                 reducer_init=self.reducer_init,
                 reducer=self.inactive_user_reducer,
                 reducer_final=self.reducer_final)

inactive_user_list = MRFindInactiveUsers.run()

def get_sentences(csv_filename,inactive_user_list):

    comments = open(csv_filename)
    comments = comments.readlines()
    list_of_sentences = []

    for comment_info in comments:
        comment_info = [x.strip() for x in comment_info.split(',')]
        comment = ''.join(comment_info[:-20])
        username = comment_info[-17]
        if username != '[deleted]':
            doc = nlp(comment)
            for sentence in doc.sents:
                list_of_sentences.append((username, str(sentence)))

    return list_of_sentences

def make_sentence_pairs(list_of_sentences):
    '''   
    [(username, sentence), (username, sentence), (...,...),...]
    '''

    with open("output.csv",'wb') as f:
        writer = csv.writer(f, dialect='excel')
        for sentence1 in list_of_sentences:
            for sentence2 in list_of_sentences:
                if sentence1[0] != '[deleted]' and sentence2[0] != '[deleted]':
                    if sentence1[0] != sentence2[0]:
                        writer.writerow([sentence1[0], sentence1[1], sentence2[0], sentence2[1]])


class MRUserbyUserMatrix(MRJob):

    '''
    n = unique_users
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

        sentiment_score = sentiment_calculator(sentence1, sentence2)

        yield (sentence1_user, sentence2_user), sentiment_score
        yield (sentence2_user, sentence1_user), sentiment_score        

    def combiner(self, users, scores):

        yield users, reduce(operator.mul, (scores), 1)

    def reducer_init(self):

        self.user_score_matrix = np.zeros((n*n))
    
    def reducer(self, user, count):

        user_row_index = self.user_dict[user[0]]
        user_col_index = self.user_dict[user[1]]
        self.user_score_matrix[user_row_index, user_col_index] = count

    def reducer_final(self):

        yield self.user_score_matrix, None

    def steps(self):

        return [
          MRStep(mapper_init=self.mapper_init,
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
                 reducer_init=self.reducer_init,
                 reducer=self.inactive_user_reducer,
                 reducer_final=self.reducer_final)