from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import re
import json
#import spacy
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import linalg, optimize, sparse
import ast


'''
main, CSVtoList, and get comments are not used anymore because their functions
were switched to MRFunctions instead. They are kept because they outline the idea
of how the MRJob for those tasks are run.
'''

def main(csv_filename, active_filename):

    active_user_list = CSVtoList(active_filename)

    comments = open(csv_filename)
    comments = comments.readlines()

    list_of_comments = []

    for line in comments:

        line = line.split(',')
        user = line[0]

        if user in active_user_list:
            comment = line[1]
            comment = comment.strip()
            doc = nlp(comment)
            list_of_comments.append([user, doc, doc.vector])

    make_comment_pairs(list_of_comments)


def CSVtoList(csv_filename):
    '''
    This gets the csv file of inactive users and returns them in a list form
    '''
    active = open(csv_filename)
    active = active.readlines()

    active_user_list = []
    for user in active:
        user = user.strip()
        user_edit = re.findall(r'"\\"(.*?)"', user)
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

        line = line.split(',')
        user = re.findall(r'"(.*)',line[0])[0]

        if user in active_user_list:
            comment = re.findall(r'(.*?)"\n', line[1])[0]
            comment = comment.strip()
            doc = nlp(comment)
            list_of_comments.append([user, doc, doc.vector])

    return list_of_comments

def make_comment_pairs(all_sentences_filename):
    '''
    Takes a list of all sentences and its user and creates a csv that 
    has every possible pair of sentences.

    Input:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
   
    Returns:
        Csv with this format --> sentence 1, user of sentence 1, vector of sentence 1, sentence 2, user of sentence 2, vector of sentence 2
    '''

    all_comments = open(all_sentences_filename)
    all_comments = all_comments.readlines()
    list_of_comments = len(all_comments) * ['']
    
    for ind, line in enumerate(all_comments):
        line = line.split(',')
        user = re.findall(r'"(.*?)"', line[0])[0]
        comment = re.findall(r'"(.*?)"', line[1])[0]
        vector = re.sub(r'\s+', ',', line[2])
        vector = re.findall(r'\[,?(.*?)"\]', vector)[0]
        vector = vector.replace('\\n', '')
        vector = '[' + vector
        #print('made it')
        vector = ast.literal_eval(vector)

        list_of_comments[ind] = (user, comment, vector)

    with open("comment_pairs.csv",'w', newline='') as f:
        writer = csv.writer(f, delimiter = '|')
        for comment1 in list_of_comments:
            for comment2 in list_of_comments:
                if comment1[0] != '[deleted]' and comment2[0] != '[deleted]':
                    if comment1[0] != comment2[0]:
                        writer.writerow([[comment1[0], comment1[1]] + comment1[2] + [comment2[0], comment2[1]] +  comment2[2]])

def find_matrix_size(csv_filename):
    '''
    Give the csv that gives the information of the matrix to be made, the function
    returns the size of the matrix needed for initialization. 
    '''
    matrix_size = 0

    matrix_csv = open(csv_filename)
    matrix_csv = matrix_csv.readlines()

    for line in matrix_csv:
        line = line.strip()
        value = re.findall(r',(.*?),', line)[0]
        value = ast.literal_eval(value)
        if value > matrix_size:
            matrix_size = value

    #print(matrix_size)
    return matrix_size + 1


def matrix_maker(csv_filename):
    '''
    Given the matrix information csv, this function returns a sparse matrix which is
    user by user, and the value is the similarity score that the users have with
    each other.
    ''' 
    user_index_dict = {}
    matrix_size = find_matrix_size(csv_filename)
    matrix = sparse.dok_matrix((matrix_size, matrix_size))
    matrix_info = open(csv_filename)
    matrix_info = matrix_info.readlines()

    for entry in matrix_info:
        user_x = re.findall(r'\["(.*?)"', entry)[0]
        user_y = re.findall(r',"(.*?)"', entry)[0]
        x = re.findall(r',(.*?),', entry)[0]
        x = ast.literal_eval(x)
        y = re.findall(r',(\d+)\]', entry)[0]
        y = ast.literal_eval(y)
        score = re.findall(r'\t(.*?)\n', entry)[0]
        score = ast.literal_eval(score)

        matrix[x,y] = score

        if user_x not in user_index_dict:
            user_index_dict[x] = user_x
        if user_y not in user_index_dict:
            user_index_dict[y] = user_y 

    return matrix
