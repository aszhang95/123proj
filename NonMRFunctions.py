from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import csv
import re
import json

def CSVtoList(csv_filename):
    '''
    This gets the csv file of inactive users and returns them in a list form
    '''
    inactive = open(csv_filename)
    inactive = inactive.readlines()

    inactive_user_list = []
    for user in inactive:
        user = user.strip()
        user_edit = re.findall(r'"\\"(.*?)"', user)
        inactive_user_list.append(user_edit[0])

    return inactive_user_list

def get_sentences(csv_filename,inactive_user_list):
    '''
    This takes the reddit csv file and returns a list of all the sentences with its user

    Returns:
        List of this format --> [(username, sentence), (username, sentence), (...,...),...]
    '''

    list_of_sentences []

    with open('file') as f:
        for line in f:
            data = json.loads(line)
            user = data["author"]
            if user != "[deleted]":
                if user not in inactive_user_list:
                    comment = data["body"]
                    comment = comment.strip()
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

def matrix_maker(csv_filename, matrix_size):

    matrix = np.zeros(matrix_size,matrix_size)
    matrix_info = open(csv_filename)
    matrix_info = matrix_info.readlines()

    for entry in matrix_info:

        entry = entry.split(',')
        x = re.findall(r'"\\"(.*?)', entry)
        y = re.findall(r'.*?(.*?)\)', entry)
        score = re.findall(r'(.*?)')
        score = int(score[0])

        matrix_info[x[0],y[0]] = score

    return matrix


