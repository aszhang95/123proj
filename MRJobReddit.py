from mrjob.job import MRJob
from mrjob.step import MRStep
import spacy

nlp = spacy.load('en')

class MRFindInactiveUsers(MRJob):

    def inactive_user_mapper(self, _, comment):

        user = comment[3]
        text = comment[0]
        yield user, 1
    
    def inactive_user_combiner(self, user, count):

        yield user, sum(score)
    
    def inactive_user_reducer(self, user, count):

        if count < 5:
            yield user, None

    def reducer_init(self):

        self.inactive_user_list = []

    def reducer(self, user, count):

        if count < 5:
            self.inactive_user_list.append(user)

    def reducer(self):

        yield self.inactive_user_list, None

    def steps(self):

        return [
          MRStep(reducer_init=self.reducer_init,
                 mapper=self.inactive_user_mapper,
                 combiner=self.inactive_user_combiner,
                 reducer=self.inactive_user_reducer,
                 reducer_final=self.reducer_final)

inactive_user_list = MRFindInactiveUsers.run()

class GetUniquePairs(MRJob):

    def mapper(self, _, comment):

        user = comment[3]
        text = comment[0]
        doc = nlp(text)
        for sentence in nlp.sentences(doc):
            yield user, sentence

    def 
