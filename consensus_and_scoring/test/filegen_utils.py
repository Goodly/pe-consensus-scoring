import json
import pandas as pd
from dummy_data import dummy_data
import test_utils
import re

#this is gonna be a helper class that all the fixtures can use
#todo make something like this for every other intermediate file
class IAA_task(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'iaa'
        super().__init__(*args, **kwargs)

    #todo add functions we'd only want to use on IAA files
    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        if 'namespace' in keys and 'question_Number' in keys:
            answer = 0
            question = params['question_Number']
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            if 'agreed_Answer' not in keys:
                answer = params['agreed_Answer']
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
        return new_row

    def set_agreement_score(self, value):
        self.set_row('agreement_score', value)

class adjudicator(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'adjudicator'
        super().__init__(*args, **kwargs)

    # todo add functions we'd only want to use on adjudicator files
    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        if 'topic_name' in keys and 'namespace' in keys:
            topic = params['topic_name']
            question = int(topic[3:5])
            answer = int(topic[6:])
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
        else:
            raise NameError('Params', params, ' must include a value for namespace and topic_name')


        return new_row


class datahunt(dummy_data):
    #NOTE: datahunt needs 2 rows at least; otherwise it'll crash the data import function.
    def __init__(self, *args, **kwargs):
        #capitalize so files match tagworks output
        self.filetype = 'DataHunt'
        super().__init__(*args, **kwargs)
        #TODO implement different size text files
        test_utils.make_text_data(self.article_id)

    def name_outfile(self, filename):
        filename = filename+'-Task'
        return filename
    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        if 'answer_label' in keys and 'namespace' in keys:
            a_label = params['answer_label']
            o = re.search('Q(.*?)\.A(.*)', a_label)
            question = int(o.group(1))
            answer = int(o.group(2))
            new_row['question_label'] = 'T1.Q'+str(question)
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
            topic_name = test_utils.get_schema_col_val(schema_sha256, 'topic_name')
            new_row['topic_name'] = topic_name
        else:
            raise NameError('Params',params,' must include a value for namespace and answer_label')
        if 'start_pos' in keys and 'end_pos' in keys:
            new_row['target_text'] = test_utils.open_text_file(self.article_id, params['start_pos'], params['end_pos'])
        elif 'startpos' in keys or 'end_pos' in keys:
            raise NameError('Params must have both a start_pos and an end_pos, or neither')

        return new_row


if __name__ == '__main__':
    #this is broken cause it's not a path data
    i =  IAA_task('../../test_data')
    i.add_row({'agreed_Answer':79, 'target_text': 'text was targeted'})
    i.export()
