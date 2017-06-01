from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import re
import json
import spacy
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import linalg, optimize, sparse
import ast

nlp = spacy.load('en')

def main(json_filename, csv_filename):

    active_user_list = CSVtoList(csv_filename)

    comments = open(csv_filename)
    comments = comments.readlines()

    list_of_comments = []

    for line in comments:

        user = line[0]

        if user in active_user_list:
            comment = line[1]
            comment = comment.strip()
            doc = nlp(comment)
            list_of_comments.append([user, doc, doc.vector])

    make_sentence_pairs(list_of_comments)

def CSVtoList(csv_filename):
    '''
    This gets the csv file of inactive users and returns them in a list form
    '''
    active = open(csv_filename)
    active = active.readlines()

    active_user_list = []
    for user in active:
        user = user.strip()
        user_edit = re.findall(r'"(.*?)"', user)
        active_user_list.append(user_edit[0])

    return active_user_list

def get_comments(csv_filename,active_user_list):
    '''
    This takes the reddit csv file and returns a list of all the sentences with its user

    Returns:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
    '''

    comments = open(csv_filename)
    comments = comments.readlines()
    list_of_comments = []

    for line in comments:

        user = line[0]

        if user in active_user_list:
            comment = line[1]
            comment = comment.strip()
            doc = nlp(comment)
            list_of_comments.append([user, doc, doc.vector])

    return list_of_comments

def make_comment_pairs(list_of_comments):
    '''
    Takes a list of all sentences and its user and creates a csv that 
    has every possible pair of sentences.

    Input:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
   
    Returns:
        Csv with this format --> sentence 1, user of sentence 1, vector of sentence 1, sentence 2, user of sentence 2, vector of sentence 2
    '''
    
    #export to a json because it should hold object values?

    with open("comment_pairs.csv",'w', newline='') as f:
        writer = csv.writer(f, delimiter = '|')
        for comment1 in list_of_comments:
            for comment2 in list_of_comments:
                if comment1[0] != '[deleted]' and comment2[0] != '[deleted]':
                    if comment1[0] != comment2[0]:
                        comment1[1] = str(comment1[1]).replace(",", " ")
                        comment2[1] = str(comment2[1]).replace(",", " ")
                        writer.writerow([[comment1[0], comment1[1]] + list(comment1[2]) + [comment2[0], comment2[1]] +  list(comment2[2])])


def matrix_maker(csv_filename, matrix_size):

    matrix = sparse.dok_matrix((matrix_size, matrix_size))
    matrix_info = open(csv_filename)
    matrix_info = matrix_info.readlines()

    for entry in matrix_info:

        x = re.findall(r'\[(.*?),', entry)[0]
        x = ast.literal_eval(x)
        y = re.findall(r',(.*?)\]', entry)[0]
        y = ast.literal_eval(y)
        score = re.findall(r'\t(.*?)\n', entry)[0]
        score = ast.literal_eval(score)

        matrix[x,y] = score

    return matrix

