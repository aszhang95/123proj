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
        user = parts[0]
        #print ("helooo")

        # user = data["author"]
        if user != "[deleted]":
            #print ("hi")

            # comment = data["body"]
            comment = parts[1]
            comment = comment.strip()
            sentiments = sentiment(comment)

            if sentiments:

                #print ("hello")
                for iden, is_political, score in sentiments:

                    if score:

                        if is_political:
                            yield (user, iden), score


                        yield is_political, score

            #entity_scores = entity.sentiment(comment)
            #for political, score in entity_scores:
                #yield political, score
            

    def combiner(self, is_political, scores):
        sum_ex = 0
        sum_ex2 = 0
        n = 0
        heights = [0] * 20
        for score in scores:
            n += 1
            sum_ex += score
            sum_ex2 += score ** 2
            temp = score * 10
            if str(temp)[0] == '-':
                if str(temp)[2] != '.':
                    heights[9 - int(str(temp)[1])] += 1
                else: 
                    if str(temp)[1] != '.':
                        heights[19] += 1
                    else:
                        heights[10 + int(str(temp)[1])] += 1

        yield is_political, (sum_ex, sum_ex2, n, heights)

    def reducer(self, is_political, ex_ex2_n_heights):
        sum_ex = 0
        sum_ex2 = 0
        sum_n = 0
        sum_heights = [0] * 20
        for ex, ex2, n, heights in ex_ex2_n_heights: 
            sum_ex += ex
            sum_ex2 += ex2
            sum_n += n
            for i in range(0,20):
                #pass
                sum_heights[i] += heights[i]
                print(heights[i])

        yield is_political, (sum_ex, sum_ex2, sum_n, sum_heights)

    def reducer_stddev(self, is_political, value):
        ex_ex2_n_heights = tuple(value)
        print(ex_ex2_n_heights)
        mean = ex_ex2_n_heights[0][0] / ex_ex2_n_heights[0][2]
        n = ex_ex2_n_heights[0][2]
        sum_ex = ex_ex2_n_heights[0][0]
        sum_ex2 = ex_ex2_n_heights[0][1]
        x = np.arange(20)
        plt.bar(x, height= ex_ex2_n_heights[0][3])
        plt.xticks(x, ['-1.0 to -.9','-.9 to -.8','-.8 to -.7','-.7 to -.6','-.6 to -.5','-.5 to -.4','-.4 to -.3','-.3 to -.2','-.2 to -.1','-.1 to 0','0 to .1','.1 to .2','.2 to .3','.3 to .4','.4 to .5','.5 to .6','.6 to .7','.7 to .8','.8 to .9','.9 to 1.0'])
        #print(n)
        print('done!')
        print(is_political, ((sum_ex2 - ((sum_ex) ** 2) / n) / n, n))
        yield is_political, ((sum_ex2 - ((sum_ex) ** 2) / n) / n, n)
        plt.show()

    def steps(self):
        return [
          MRStep(
                 mapper=self.mapper,
                 combiner=self.combiner,
                 reducer=self.reducer),
          MRStep(reducer=self.reducer_stddev)]

        #https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Na.C3.AFve_algorithm


if __name__ == '__main__':
  IsPolitical.run()
  