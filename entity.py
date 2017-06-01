import json
import requests as req
import spacy
import re
from textblob.en.sentiments import PatternAnalyzer
import numpy as np

nlp = spacy.load('en')

words = set(['politic'])

types = {'Person':'PERSON', 'Event':'EVENT', 'Organization':'ORG', 'Place':'LOC'}

def sentiment(comment, subjectivity = True):
    doc = nlp(comment)

    outs = []

    for ind, entity in enumerate(doc.ents):
        if entity.label_ in types.values():
            iden, political_entity = is_political(entity.text)
            if iden:
                analyzed = PatternAnalyzer.analyze(PatternAnalyzer, str(entity.sent))
                score = None

                if subjectivity:
                    score = analyzed.polarity * analyzed.subjectivity
                else:
                    score = analyzed.polarity

                outs.append((iden, political_entity, score))

    return outs





def is_political(ent_text):

    api_key = 'AIzaSyAMSkyNxAUbhtlvfWOKGJAO8w1hbj2WXC0'
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    ent_text = re.sub("[^\w]", "", ent_text)
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
    political_entity = False
    if 'itemListElement' in data.keys():
        for item in data['itemListElement']:
            description = None
            try:
                description = item['result']['detailedDescription']['articleBody']

            except:
                try:
                    description = item['result']['description']
                except:
                    #ERROR: NO DESCRIPTION
                    #print(item)
                    description = ''
            try:
                description += ' ' + item['result']['name']
            except:
                pass

            political = sum([word in description for word in words])

            try:
                right_type = sum([t in types.keys() for t in item['result']['@type']])
            except:
                pass
                #ERROR IN TYPE

            if political and right_type:
                if political > score:
                    try:
                        out = item['result']['name']
                        score = political
                        political_entity = True
                    except:
                        pass
                        #ERROR IN ID/NAME
    else:
        pass
        #ERROR: NO RESULTS FOR {}'.format(ent_text))
            
    if not out:
        try:
            entity = data['itemListElement'][0]['result']
            out = entity['name']
        except:
            pass
            #print('\n NOTHING FOUND FOR ENTITY: {}'.format(ent_text))
    return (out, political_entity)
