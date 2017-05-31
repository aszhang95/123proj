import json
import requests as req
import spacy
import spacy
from textblob.en.sentiments import PatternAnalyzer
import numpy as np

nlp = spacy.load('en')

words = set(['politic'])

types = {'Person':'PERSON', 'Event':'EVENT', 'Organization':'ORG', 'Place':'LOC'}

def sentiment(comment, subjectivity = True):
    doc = nlp(comment)

    outs = np.zeros(len(doc.ents), dtype = tuple)

    for ind, entity in enumerate(doc.ents):
        if entity.label_ in types.values():
            iden = is_political(entity.text)
            analyzed = PatternAnalyzer.analyze(PatternAnalyzer, str(entity.sent))
            score = None

            if subjectivity:
                score = analyzed.polarity * analyzed.subjectivity
            else:
                score = analyzed.polarity

            outs.append((iden[0], score))

    return outs





def is_political(ent_text):

    api_key = 'AIzaSyAMSkyNxAUbhtlvfWOKGJAO8w1hbj2WXC0'
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    params = [
        ('query', ent_text),
        ('limit', 5),
        ('indent', True),
        ('key', api_key)
    ]


    page = req.get(url = service_url, params = params)
    data = json.loads(page.text)
    out = None
    score = 0
    for item in data['itemListElement']:
        description = None
        try:
            description = item['result']['detailedDescription']['articleBody'] + ' ' + item['result']['name']

        except:
            description = item['result']['description']

        political = sum([word in description for word in words])

        right_type = sum([t in types.keys() for t in item['result']['@type']])

        if political and right_type:
            if political > score:
                out = (item['result']['@id'], item['result']['name'])
                score = political
                political_entity = True
    
    if not out:
        entity = data['itemListElement'][0]['result']
        out = (entity['@id'], entity['name'])
        political_entity = False
    return out, political_entity
