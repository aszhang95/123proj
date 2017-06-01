"""
    This code creates a numpy matrix using Reddit data via MapReduce
"""

import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import *
import re
import numpy as np 
import random as r
import json
import entity

user_dict, word_dict = {}, {}
user_counter, word_counter = 0, 0

class MatrixMR(MRJob):

    # Mapper reads in lines from the .csv file and yields the tuple
    def mapper(self, _, line):
        data = json.loads(line)
        user = data["author"]
        if user != "[deleted]":
            comment = data["body"]
            comment = comment.strip()
            # print("username: {}, comment: {}".format(user, comment))
            # run sentiment analysis function on the comment
            # returns a list of tuples

            #simulate output using list of (word, random_float) pairs
            wordList = re.sub("[^\w]", " ", comment).split()
            # print("\n\nlist of words: {}".format(wordList))
            outlist = []
            for word in wordList:
                outlist.append((word, r.random()))
            # print(outlist)
            # outlist = entity.sentiment(comment)

            yield user, outlist


    def reducer(self, user, pairs_list):
        # according to the TA from pa1, combiner doesn't completely combine everything
        total_list = []
        for l in pairs_list:
            word = l[0]
            if word in word_dict
            total_list += l # duplicates are good 

        # print("User: {}\n pairs_list: {}\n\n".format(user, total_list))
        user_dict[user] = user_counter
        user_counter += 1

        yield user, total_list



if __name__ == '__main__':
    MatrixMR.run()

