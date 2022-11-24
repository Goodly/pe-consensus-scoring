import json
from mockAnnotatorData.dummy_data import dummy_data
import mockAnnotatorData.test_utils as test_utils
import re

#this is gonna be a helper class that all the fixtures can use
class IAA_task(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'iaa'
        super().__init__(*args, **kwargs)

    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        if 'namespace' in keys and 'question_Number' in keys:
            answer = 0
            question = params['question_Number']
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            if 'agreed_Answer' in keys:
                answer = params['agreed_Answer']
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
        return new_row

    def set_agreement_score(self, value):
        self.set_row('agreement_score', value)

class dep_iaa(IAA_task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_out_name(self, filetype, source_task_id):

        return 'Dep_'+filetype + '_' + self.namespace+'_'+source_task_id + '-Task.csv'

class adjudicator(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'adjudicator'
        super().__init__(*args, **kwargs)

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
        test_utils.make_text_data(self.article_id)

    def set_out_name(self, filetype, source_task_id):
        return filetype + '_' + source_task_id + '-Task.csv'

    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        if 'answer_label' in keys and 'namespace' in keys and 'contributor_uuid' in keys:

            a_label = params['answer_label']
            o = re.search('Q(.*?)\.A(.*)', a_label)
            question = int(o.group(1))
            answer = int(o.group(2))
            q_label = 'T1.Q'+str(question)
            new_row['question_label'] = q_label
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
            new_row['question_label'] = 'T1.Q'+str(question)
            topic_name = test_utils.get_schema_col_val(schema_sha256, 'topic_name')
            new_row['topic_name'] = topic_name
        else:
            raise NameError('Params',params,' must include a value for namespace, answer_label, and contributor_uuid')

        if 'start_pos' in keys and 'end_pos' in keys:
            if not test_utils.schema_has_hl(schema_sha256, a_label):
                print("Schema doesn't support highlight for question/answer", a_label)
            else:
                new_row['target_text'] = test_utils.open_text_file(self.article_id, params['start_pos'], params['end_pos'])
                a_df = self.df[self.df['answer_label'] == a_label]

                if len(a_df) >0:
                    max_hl = a_df['highlight_count'].max()
                else:
                    max_hl = 0
                max_hl += 1
                mask = self.df['answer_label'] == a_label
                new_row['highlight_count'] = max_hl
                self.df['highlight_count'][mask] = max_hl
        elif 'startpos' in keys or 'end_pos' in keys:
            raise NameError('Params must have both a start_pos and an end_pos, or neither')
        if 'quiz_task_uuid' not in keys:
            new_row['quiz_task_uuid'] = self.source_task_id
        return new_row

    def sort(self):
        self.df = self.df.sort_values(by=['question_label'])

class weighted(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'weighted'
        super().__init__(*args, **kwargs)
        self.schema = "default_schema"

    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        #Schema can be: Language, Reasoning, Evidence, Probability, Holistic, Sources
        if 'schema' in keys:
            self.schema = params['schema']

        if 'namespace' in keys and 'Question_Number' in keys and 'agreement_adjusted_points' in keys and 'Answer_Number':
            answer = 0
            question = params['Question_Number']
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            answer = params['Answer_Number']
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
        else:
            raise NameError('Params',params,' must include a value for namespace, Question_Number, and Answer_Number, and agreement_adjusted_points')
        new_row['schema'] = self.schema
        return new_row

    def set_out_name(self, filetype, source_task_id):
        return filetype + '_' + source_task_id + '-Task.csv'

class tua(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'TUA'
        super().__init__(*args, **kwargs)
        test_utils.make_text_data(self.article_id)

    def fill_in_logic(self, new_row, params):
        keys = params.keys()

        return new_row

    def set_out_name(self, filetype, source_task_id):
        return filetype + '_' + source_task_id + '-Task.csv'

class point_assignment(dummy_data):
    def __init__(self, *args, **kwargs):
        self.filetype = 'point_assignment'
        super().__init__(*args, **kwargs)
        self.schema = "default_schema"

    def fill_in_logic(self, new_row, params):
        keys = params.keys()
        #Schema can be: Language, Reasoning, Evidence, Probability, Holistic, Sources


        if 'namespace' in keys and 'Question_Number' in keys and 'points' in keys and 'Answer_Number':
            question = params['Question_Number']
            schema_sha256 = test_utils.sha256_from_namespace(params['namespace'])
            answer = params['Answer_Number']
            ans_id, ans_text, q_text = test_utils.get_schema_data(schema_sha256, question, answer)
            new_row['schema_sha256'] = schema_sha256
            new_row['answer_uuid'] = ans_id
            new_row['answer_text'] = ans_text
            new_row['question_text'] = q_text
        if 'article_text_length' not in keys:
            new_row['article_text_length'] = 1000
        else:
            raise NameError('Params',params,' must include a value for namespace, Question_Number, and Answer_Number, and agreement_adjusted_points')
        new_row['schema'] = self.schema
        return new_row

    def set_out_name(self, filetype, source_task_id):
        return 'SortedPts.csv'

if __name__ == '__main__':
    #this is broken cause it's not a path data
    with open('../test/test_config.json') as json_file:
        config = json.load(json_file)
    dh_path = test_utils.make_test_directory(config, 'mn_dh_')
    dh = datahunt(out_folder=dh_path, source_task_id='dh13', article_num='520', article_text_length=2900)
    for i in range(9):
        for j in range(7):
            dh.add_row({'answer_label': 'T1.Q1.A' + str(j), 'namespace': 'Covid_Languagev1.1',
                        'contributor_uuid': 'User' + str(i), 'start_pos': 10 * j, 'end_pos': 12 * j})
    for i in range(9):
        for j in range(7):
            dh.add_row({'answer_label': 'T1.Q1.A' + str(j), 'namespace': 'Covid_Languagev1.1',
                        'contributor_uuid': 'User' + str(i), 'start_pos': 15 * j, 'end_pos': 17 * j})