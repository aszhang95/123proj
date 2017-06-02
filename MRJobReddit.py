from mrjob.job import MRJob
from mrjob.step import MRStep
import spacy
import sys
import csv
import numpy as np
from functools import reduce
import operator

nlp = spacy.load('en')
#pip install mr3px
#https://stackoverflow.com/questions/31032885/mrjob-and-python-csv-file-output-for-reducer/31331870#31331870
#https://pypi.python.org/pypi/mr3px
#python3 taskx.py csvfile > newcsvfilename

class MRFindInactiveUsers(MRJob):
    '''
    This will yield the inactive users (less than 5 comments) into a csv file
    as user, None
    ''' 

    def inactive_user_mapper(self, _, comment):
        print(comment)
        comment_info = [x.strip() for x in comment.split(',')]
        comment = ''.join(comment_info[:-20])
        username = comment_info[-17]
        if username != '[deleted]':
            yield username, 1
    
    def inactive_user_combiner(self, user, count):

        yield user, sum(count)

    def inactive_user_reducer(self, user, count):
        #shouldn't this just be count?
        if sum(count) < 5:
            yield (None, user)

    def steps(self):

        return [
          MRStep(
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
                 reducer=self.inactive_user_reducer)]

if __name__ == '__main__':
    MRFindInactiveUsers.run()

def CSVtoList(csv_filename):
    '''
    This gets the csv file of inactive users and returns them in a list form
    '''
    inactive = open(csv_filename)
    inactive = inactive.readlines()

    #this part unnecesarily adds computation. readlines turns things into lists automatically
    #by adding a for loop AND appending everytime this is a super unnecesary add to memory
    inactive_user_list = []
    for user in inactive:
        user = user.strip()
        inactive_user_list.append(user)

    return inactive_user_list

def get_sentences(csv_filename,inactive_user_list):
    '''
    This takes the reddit csv file and returns a list of all the sentences with its user

    Returns:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
    '''
    comments = open(csv_filename)
    comments = comments.readlines()
    list_of_sentences = []

    for comment_info in comments:
        comment_info = [x.strip() for x in comment_info.split(',')]
        comment = ''.join(comment_info[:-20])
        username = comment_info[-17]
        if username != '[deleted]':
            if username not in inactive_user_list:
                doc = nlp(comment)
                for sentence in doc.sents:
                    list_of_sentences.append((username, str(sentence)))

    return list_of_sentences

def make_sentence_pairs(list_of_sentences):
    '''
    Takes a list of all sentences and its user and creates a csv that 
    has every possible pair of sentences.

    Input:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
   
    Returns:
        Csv with this format --> sentence 1, user of sentence 1, sentence 2, user of sentence 2
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