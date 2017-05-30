"""
    This code creates a numpy matrix using Reddit data via MapReduce
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import *
import re
import numpy as np 
