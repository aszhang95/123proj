from mrjob.job import MRJob
from mrjob.step import MRStep
import spacy
import sys
import csv

nlp = spacy.load('en')

class MRFindInactiveUsers(MRJob):

    def inactive_user_mapper(self, _, comment):

        comment_info = [x.strip() for x in comment_info.split(',')]
        comment = ''.join(comment_info[:-20])
        username = comment_info[-17]
        if username != '[deleted]':
            yield username, 1
    
    def inactive_user_combiner(self, user, count):

        yield user, sum(score)
    
    def inactive_user_reducer(self, user, count):

        if count < 5:
            yield user, None

    def reducer_init(self):

        self.inactive_user_list = []

    def reducer(self, user, count):

        if count < 5:
            self.inactive_user_list.append(user)

    def reducer(self):

        yield self.inactive_user_list, None

    def steps(self):

        return [
          MRStep(reducer_init=self.reducer_init,
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
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
                list_of_sentences.append(sentence)

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

