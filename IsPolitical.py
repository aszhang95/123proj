from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from wikidataintegrator import wdi_core
import spacy
from SPARQLWrapper import SPARQLWrapper, JSON

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

my_first_wikidata_item = wdi_core.WDItemEngine(wd_item_id='Q5')

class IsPolitical(MRJob):
        list_of_named_entity_labels=['PERSON', 'NORP', 'ORG', 'GPE', 'LOC', 'EVENT', 'WORK_OF_ART', 'LANGUAGE']
        list_of_political_words=['politician', 'Congress', 'President', 'senator', 'State', 'Vice-President']
    def mapper(self, _, line):
        '''
        output: entity, boolean (true if political, false if non-political)
        '''
        #SPACY: determine named entities in comment
    	user=line[4]
        comment=line[0]
        nlp = spacy.load('en')
        doc = nlp(unicode(comment)) #doc = nlp(u'London is a big city in the United Kingdom.')
        for ent in doc.ents:
            if ent.label_ in list_of_named_entity_labels:
                #send entity to SPARQL
                #if entity in list_of_political_words

        #https://www.wikidata.org/wiki/Wikidata:A_beginner-friendly_course_for_SPARQL#Properties_of_the_object_.28Relative_Clauses.29
        #control find 'description'
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        sparql.setQuery("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label
            WHERE { <http://dbpedia.org/resource/Asturias> rdfs:label ?label }
        """)
        SELECT ?item ?itemLabel
WHERE
{
    ?item wdt:P31 wd:Q146 .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        for result in results["results"]["bindings"]:
            print(result["label"]["value"])

        #given a named entity, search wikidata to figure out if political. if it is, output ID(unique ID per entitiy. i.e. Trump, Donald Trump) and word
        #subreddit:
        for score in scores:
        	#score is a tuple (entity, score)
        	yield user, score

    def combiner(self, guest, visits):
        yield guest, sum(visits)
    
    def reducer(self, guest, total_visits):
        if sum(total_visits) >= 10:
            yield guest, None

if __name__ == '__main__':
  MRGuestFrequent.run()
  