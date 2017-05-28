from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from wikidataintegrator import wdi_core
import spacy
from SPARQLWrapper import SPARQLWrapper, JSON

nlp = spacy.load('en')
list_of_named_entity_labels=['PERSON', 'NORP', 'ORG', 'GPE', 'LOC', 'EVENT', 'WORK_OF_ART', 'LANGUAGE']
list_of_political_words=['politician', 'Congress', 'President', 'senator', 'State', 'Vice-President'...add more words...]

''' EXAMPLE ROW:
{
    "body": "The Lump of Labor fallacy is that the amount of labor required is FIXED.  Saying that technology increases productivity faster than the demand for new labor is created does not make that assumption.  \n\nThis is the same reason increasing minimum wage actually works, the increase in wage overtakes the increase to cost of living over the short to medium term.",
    "score_hidden": "false",
    "archived": "false",
    "name": "t1_cregzcu",
    "author": "Skyrmir",
    "author_flair_text": null,
    "downs": "0",
    "created_utc": "1432068778",
    "subreddit_id": "t5_2cneq",
    "link_id": "t3_36iav3",
    "parent_id": "t1_crebl6k",
    "score": "2",
    "retrieved_on": "1433155763",
    "controversiality": "0",
    "gilded": "0",
    "id": "cregzcu",
    "subreddit": "politics",
    "ups": "2",
    "distinguished": null,
    "author_flair_css_class": null
  } '''

class IsPolitical(MRJob):

    def mapper(self, _, line):
        '''
        output: entity, boolean (true if political, false if non-political)
        '''
        line = line.split(',')
        #SPACY: determine named entities in comment
    	user=line[-17]
        comment=line[-20]
        doc = nlp(unicode(comment)) #doc = nlp(u'London is a big city in the United Kingdom.')
        score = sids_function(comment)
        for ent in doc.ents:
            if ent.label_ in list_of_named_entity_labels:
                if IsPoliticalSidsAPI(ent):
                    yield True, (score, 1)
                if not IsPoliticalSidsAPI(ent):
                    yield False, (score, 1)
#still need to figure out how to perform stddev on the two yields

    def combiner(self, is_political, score_count):
        yield is_political, score_count
#check this. does combiner need to perform a function always?

    def reducer_init(self):
        self.sum = 0
        self.n = 0
        self.mean = self.sum/self.n
        self.sum_ex = 0
        self.sum_ex2 = 0

    def reducer(self, guest, score_count):
        for score, count in zip(score_count): #check this zip too. can zip take in generator types?
            self.sum_ex += score
            self.count += n

    def reducer_final(self):
        yield ((self.sum_ex2-((self.sum_ex) ** 2)/self.n)/self.n - 1, None) #https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
