import json
import requests as req
import spacy
import re
from textblob.en.sentiments import PatternAnalyzer
import numpy as np
import jellyfish as jf

nlp = spacy.load('en')

#list of political words to check against
words = set(['politic', 'govern', 'party', 'secretary of', 'united states', 'president', 'election', 'democr', 'republic', 'senat','representative', 'congress', 'court', 'campaign', ''])

types = {'Person':'PERSON', 'Organization':'ORG'}

def sentiment(comment, searches, subjectivity = False):
    doc = nlp(comment)

    outs = []

    for ind, entity in enumerate(doc.ents):
        #checking for entity type
        if entity.label_ in types.values():
            iden, political_entity = None, None

            #avoiding querying knowledge graph every time with running dictionary
            if entity.text in searches.keys():
                iden, political_entity = searches[entity.text]

            else:
                found = False
                for text in searches.keys():
                    #comparing with similar ones in case of typoes
                    if jf.jaro_winkler(text, entity.text) > 0.9:
                        found = True
                        searches[entity.text] = searches[text]
                        iden, political_entity = searches[entity.text]
                        break

                if not found:
                    iden, political_entity = is_political(entity.text)

            if iden:
                #get sentiment
                analyzed = PatternAnalyzer.analyze(PatternAnalyzer, str(entity.sent))
                score = None

                if subjectivity:
                    score = analyzed.polarity * analyzed.subjectivity
                else:
                    score = analyzed.polarity

                outs.append((entity.text, iden, political_entity, score))

    return outs


def is_political(ent_text):

    api_key = 'AIzaSyAMSkyNxAUbhtlvfWOKGJAO8w1hbj2WXC0'
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    ent_text = re.sub("[^(\w\s)]", "", ent_text)
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

    #parsing search results
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
                    description = ''
            try:
                description += ' ' + item['result']['name']
            except:
                #NO NAME
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
    #out only has value if entity is political, if not we assume it's the first one.
    if not out:
        try:
            entity = data['itemListElement'][0]['result']
            out = entity['name']
        except:
            #No name
            pass
            
    return (out, political_entity)
