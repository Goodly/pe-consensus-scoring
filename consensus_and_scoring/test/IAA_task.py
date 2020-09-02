import json
import pandas as pd
from dummy_data import dummy_data
#this is gonna be a helper class that all the fixtures can use
#todo make something like this for every other intermediate file
class IAA_task(dummy_data):
    def __init__(self, out_path):
        super().__init__('iaa', out_path)

    #todo add functions we'd only want to use on IAA files
    def set_agreement_score(self, value):
        self.set_row('agreement_score', value)
if __name__ == '__main__':
    #this is broken cause it's not a path data
    i =  IAA_task('../../test_data')
    i.add_row({'agreed_Answer':79, 'target_text': 'text was targeted'})
    i.export()
