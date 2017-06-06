from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import spacy
import json
from entity import *
import matplotlib.pyplot as plt
import numpy as np
from scipy.sparse import dok_matrix
from random import randint

NUM_BUCKETS = 10

#This File Does the important work of collecting named entities and sentiment associated with them.

class IsPolitical(MRJob):


    def mapper_init(self):
        self.users = dict()
        self.user_index = 0
        self.entities = dict()
        self.entity_index = 0
        self.searches = dict()
        self.comments = 0

    def mapper(self, _, line):
        self.comments += 1

        #parse line
        line_len = len(line)
        line = line[1:line_len-1]
        parts = line.split(',')
        user = parts[0]

        if user != "[deleted]":

            comment = parts[1][1:len(parts[1])-1]

            #get entities and sentiments associated with them
            sentiments = sentiment(comment, self.searches, False)

            if sentiments:

                for text, iden, is_political, score in sentiments:
                    #recording searches so as not to do redundant searches
                    self.searches[text] = (iden, is_political)
                    if score:

                        if is_political:
                            #keeping track of users and entities
                            if user not in self.users:
                                self.users[user] = self.user_index
                                self.user_index += 1
                            
                            if iden not in self.entities:
                                self.entities[iden] = self.entity_index
                                self.entity_index += 1

                            #yield for matrix data (Hypothesis 2)
                            yield (user, self.users[user], iden, self.entities[iden]), score


                        #yield for polarization data (Hypothesis 1)
                        yield is_political, score

    def combiner(self, key, value):
        scores = value

        if type(key) == bool:
            is_political = key
            sum_ex = 0
            sum_ex2 = 0
            n = 0
            heights = np.zeros(NUM_BUCKETS + 1)
            for score in scores:
                n += 1
                sum_ex += score
                sum_ex2 += score ** 2
                bucket = int((score + 1) * NUM_BUCKETS/2)
                heights[bucket] += 1

            yield is_political, (sum_ex, sum_ex2, n, heights)
        
        else:
            pair = key
            yield pair, sum(scores)


    def reducer(self, key, value):
        if type(key) == bool:
            is_political = key

            #algorithm for parallel calculation of variance is here:  https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Na.C3.AFve_algorithm
            ex_ex2_n_heights = value
            sum_ex = 0
            sum_ex2 = 0
            sum_n = 0
            sum_heights = np.zeros(NUM_BUCKETS + 1)
            for ex, ex2, n, heights in ex_ex2_n_heights: 
                sum_ex += ex
                sum_ex2 += ex2
                sum_n += n
                sum_heights += np.array(heights)

            yield is_political, (sum_ex, sum_ex2, sum_n, sum_heights)
        
        else:
            yield key, sum(value)
    

    def reducer_stddev(self, key, value):


        if type(key) == bool:
            is_political = key
            sum_ex, sum_ex2, n, sum_heights = next(value)
            mean = sum_ex / n

            #making a histogram of sentiment
            x = np.arange(NUM_BUCKETS + 1)
            plt.bar(x, height = sum_heights)
            locs = np.arange(0,NUM_BUCKETS,NUM_BUCKETS/10)
            off = NUM_BUCKETS / 2
            ticks = ['{} to {}'.format((boundary - off)/off,((boundary - off)/off) + 2/NUM_BUCKETS) for boundary in locs]
            plt.xticks(locs, ticks)
            if is_political:
                title = 'Histogram of Sentiment for Political entities'
            else:
                title = 'Histogram of Sentiment for non-political entities'
            plt.title(title)
            plt.show()
            yield is_political, ((sum_ex2 - ((sum_ex) ** 2) / n) / n, n)
            
        else:
            yield key, value

    def steps(self):
        return [
          MRStep(
                 mapper_init = self.mapper_init,
                 mapper=self.mapper,
                 combiner=self.combiner,
                 reducer=self.reducer),
          MRStep(reducer=self.reducer_stddev)]




if __name__ == '__main__':
  IsPolitical.run()
  