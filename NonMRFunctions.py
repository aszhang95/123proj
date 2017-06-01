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

    nlp_dictionary = {}
    current_index = 0

    list_of_sentences = []

    with open(json_filename) as f:
        for line in f:
            data = json.loads(line)
            user = data["author"]
            if user != "[deleted]":
                if user in active_user_list:
                    comment = data["body"]
                    comment = comment.strip()
                    doc = nlp(comment)
                    for sentence in doc.sents:
                        nlp_dictionary[current_index] = sentence
                        list_of_sentences.append((user, current_index))
                        current_index += 1

    make_sentence_pairs(list_of_sentences)

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

def get_sentences(json_filename,active_user_list):
    '''
    This takes the reddit csv file and returns a list of all the sentences with its user

    Returns:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
    '''

    list_of_sentences = []

    with open(json_filename) as f:
        for line in f:
            data = json.loads(line)
            user = data["author"]
            if user != "[deleted]":
                if user in active_user_list:
                    comment = data["body"]
                    comment = comment.strip()
                    doc = nlp(comment)
                    list_of_sentences.append((user, doc, list(doc.vector)))

    return list_of_sentences

def make_sentence_pairs(list_of_sentences):
    '''
    Takes a list of all sentences and its user and creates a csv that 
    has every possible pair of sentences.

    Input:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
   
    Returns:
        Csv with this format --> sentence 1, user of sentence 1, vector of sentence 1, sentence 2, user of sentence 2, vector of sentence 2
    '''
    
    #export to a json because it should hold object values?

    with open("sentence_pairs.csv",'w', newline='') as f:
        writer = csv.writer(f, delimiter = '|')
        for sentence1 in list_of_sentences:
            for sentence2 in list_of_sentences:
                if sentence1[0] != '[deleted]' and sentence2[0] != '[deleted]':
                    if sentence1[0] != sentence2[0]:
                        writer.writerow([sentence1[0], sentence1[1], sentence1[2], sentence2[0], sentence2[1], sentence2[2]])


def matrix_maker(csv_filename, matrix_size):

    matrix = lil_matrix(matrix_size)
    matrix_info = open(csv_filename)
    matrix_info = matrix_info.readlines()

    for entry in matrix_info:

        entry = entry.split(',')
        x = re.findall(r'"\\"(.*?)', entry)
        y = re.findall(r'.*?(.*?)\)', entry)
        score = re.findall(r'(.*?)')
        score = int(score[0].strip())

        matrix_info[x[0],y[0]] = score

    return matrix


