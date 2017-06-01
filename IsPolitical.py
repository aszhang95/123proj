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


'''
    def mapper_init(self):
        USERS = dict()
        USER_INDEX = 0
        ENTITIES = dict()
        ENTITY_INDEX = 0

        SCORE_MATRIX = dok_matrix((20000, 100000))

                            if user not in USERS.keys():
                                USERS[user] = USER_INDEX
                                USER_INDEX += 1

                            if iden not in entities.keys():
                                ENTITIES[iden] = ENTITY_INDEX
                                ENTITY_INDEX += 1
                            
                            SCORE_MATRIX[USERS[user], ENTITIES[iden]] += score
'''

class IsPolitical(MRJob):


    def mapper_init(self):
        self.users = dict()
        self.user_index = 0
        self.entities = dict()
        self.entity_index = 0

    def mapper(self, _, line):
        '''
        output: entity, boolean (true if political, false if non-political)

        import matplotlib.pyplot as plt
        import numpy as np
        x = np.arange(3)
        plt.bar(x, height= [1,2,3])
        plt.xticks(x, ['a','b','c'])
        plt.show()
        '''
        #print('heloooooo')
        # data = json.loads(line)
        line_len = len(line)
        line = line[1:line_len-1]
        parts = line.split(',')
        user = re.findall(r'\["(.*?)"', parts[0])[0]
        #print ("helooo")

        # user = data["author"]
        if user != "[deleted]":
            #print ("hi")

            # comment = data["body"]
            comment = re.findall(r'"(.*?)"\]', parts[1])[0]

            sentiments = sentiment(comment)

            if sentiments:

                #print ("hello")
                for iden, is_political, score in sentiments:

                    if score:

                        if is_political:

                            if user not in self.users:
                                self.users[user] = self.user_index
                                self.user_index += 1
                            
                            if iden not in self.entities:
                                self.entities[iden] = self.entity_index
                                self.entity_index += 1

                            yield (self.users[user], self.entities[iden]), score


                        yield is_political, score

    def combiner(self, key, value):
        scores = value
        if type(key) == bool:
            is_political = key
            sum_ex = 0
            sum_ex2 = 0
            n = 0
            heights = np.zeros(201)
            for score in scores:
                n += 1
                sum_ex += score
                sum_ex2 += score ** 2
                bucket = int((score + 1) * 100)
                heights[bucket] += 1

            yield is_political, (sum_ex, sum_ex2, n, heights)
        
        else:
            pair = key
            yield pair, sum(scores)


    def reducer(self, key, value):
        if type(key) == bool:
            is_political = key
            ex_ex2_n_heights = value
            sum_ex = 0
            sum_ex2 = 0
            sum_n = 0
            sum_heights = np.zeros(201)
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
            x = np.arange(201)
            plt.bar(x, height = sum_heights)
            locs = np.arange(0,200,10)
            ticks = ['{} to {}'.format((boundary - 100)/100,((boundary - 100)/100) + 0.01) for boundary in locs]
            plt.xticks(locs, ticks)
            if is_political:
                title = 'Histogram of Sentiment for Political entities'
            else:
                title = 'Histogram of Sentiment for non-political entities'
            plt.title(title)
            plt.show()
            #plt.savefig('{}.png'.format(title))
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

        #https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Na.C3.AFve_algorithm


if __name__ == '__main__':
  IsPolitical.run()
  