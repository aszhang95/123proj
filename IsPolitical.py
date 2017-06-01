from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import spacy
import json
from entity import *
import matplotlib.pyplot as plt
import numpy as np
#from random import randint

nlp = spacy.load('en')

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
        data = json.loads(line)
        #print ("helooo")

        user = data["author"]
        if user != "[deleted]":
            #print ("hi")

            comment = data["body"]
            comment = comment.strip()
            sentiments = sentiment(comment)
            #print ("hello")
            for is_political, score in sentiments:
                yield is_political, score

            #entity_scores = entity.sentiment(comment)
            #for political, score in entity_scores:
                #yield political, score
            
    def combiner(self, is_political, scores):
        sum_ex = 0
        sum_ex2 = 0
        n = 0
        heights = [0,0,0,0,0,0,0,0,0,0]
        for score in scores:
            n += 1
            sum_ex += score
            sum_ex2 += score ** 2
            temp = int(str(score)[0])
            heights[temp] += 1

        yield is_political, (sum_ex, sum_ex2, n, heights)

    def reducer(self, is_political, ex_ex2_n_heights):
        sum_ex = 0
        sum_ex2 = 0
        sum_n = 0
        sum_heights = [0,0,0,0,0,0,0,0,0,0]
        for ex, ex2, n, heights in ex_ex2_n_heights: 
            sum_ex += ex
            sum_ex2 += ex2
            sum_n += n
            for i in range(0,10):
                sum_heights[i] += heights[i]
                print(heights[i])

        yield is_political, (sum_ex, sum_ex2, sum_n, sum_heights)

    def reducer_stddev(self, is_political, ex_ex2_n_heights):
        mean = ex_ex2_n_heights[0] / ex_ex2_n_heights[2]
        n = ex_ex2_n_heights[2]
        sum_ex = ex_ex2_n_heights[0]
        sum_ex2 = ex_ex2_n_heights[1]
        x = np.arange(10)
        plt.bar(x, height= ex_ex2_n_heights[3])
        plt.xticks(x, ['0-.1','.1-.2','.2-.3','.3-.4','.4-.5','.5-.6','.6-.7','.7-.8','.8-.9','.9-1.0'])
        print(n)
        print(is_political, ((sum_ex2 - ((sum_ex) ** 2) / n) / n, n))
        yield is_political, ((sum_ex2 - ((sum_ex) ** 2) / n) / n, n)

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
  