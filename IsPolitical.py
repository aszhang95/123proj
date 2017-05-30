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
    	user=line[-17]
        comment=line[-20]
        doc = nlp(unicode(comment)) 
        score = sids_function(comment)
        for ent in doc.ents:
            if ent.label_ in list_of_named_entity_labels:
                if IsPoliticalSidsAPI(ent):
                    yield True, score
                if not IsPoliticalSidsAPI(ent):
                    yield False, score

    def combiner(self, is_political, scores):
        sum_ex = 0
        sum_ex2 = 0
        n = 0
        for score in scores:
            n += 1
            sum_ex += score
            sum_ex2 += score ** 2

        yield is_political, (sum_ex, sum_ex2, n)

    def reducer(self, is_political, ex_ex2_n):
        sum_ex = 0
        sum_ex2 = 0
        n = 0
        for ex, ex_2, n in ex_ex2_n: 
            sum_ex += ex
            sum_ex2 += ex2
            n += n
        yield is_political, (sum_ex, sum_ex2, n)

    def reducer_final(self, is_political, total_ex_ex2_n):
        mean = total_ex_ex2_n[0] / total_ex_ex2_n[2]
        n = total_ex_ex2_n[2]
        sum_ex = total_ex_ex2_n[0]
        sum_ex2 = total_ex_ex2_n[1]
        yield is_political, (sum_ex2 - ((sum_ex) ** 2) / n) / n 
