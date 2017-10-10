# 123proj
CMSC 12300 Project NLP on reddit comments using MapReduce

ActiveCSVMaker.py finds users with more than X comments, and filters out some percentage to keep dataset managable. 

SENTIMENT
MRFilterComments.py filters comments based on active users
IsPolitical.py performs does all the data aggregation
entity.py does all the NLP
matrix_operations.py performs all the clustering.

SIMILARITY
Directions:

In terminal: python3 ActiveCSVmaker.py data_formatted.csv > active.csv
MRMakeSentences.py filters comments and gets comment vectors
Make COmment pairs
	a) run NonMRFunctions.py
	b) make_comment_pairs('all_comments.csv')
matrixcsvmaker.py turns comment pairs into data for matrix via dot products
makes the matrix:
matrix_maker('matrixinfo.csv')
