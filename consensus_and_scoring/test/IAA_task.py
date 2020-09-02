import json
import pandas as pd

#this is gonna be a helper class that all the fixtures can use
#todo make something like this for every other intermediate file
class IAA_task:
    #todo add other optional params, so can sync up ids across filetypes
    def __init__(self, out_path):
        #out_path is a path object
        self.out_path = out_path.mkdir('test_iaa')
        #automatically make every row have this task id
        self.lock_source_task_id = True
        #generate the id every row in this file will have
        self.source_task_id = 'xuaixs7a8xnia89'
        #todo have similar process for article_uuid, etc
        with open('test_config.json') as json_file:
            data = json.load(json_file)

        self.base_row = data['file_type']['iaa']
        self.cols = self.base_row.keys()
        self.df = pd.DataFrame(columns = self.cols)

    #todo add helper functions to make the defaults more defaulty
    def add_row(self, params = None):
        new_row = self.base_row
        if params != None:
            for p in params.keys():
                if p not in self.cols:
                    raise Exception("Column name entered"+p+" isn't a column name in the IAA table")
                new_row[p] = params[p]
                #todo: logically fill in other rows; ie given schema name fill in schema_id, given answer/schema fill in answer_uuid
        print(new_row)
        if self.lock_source_task_id:
            new_row['source_task_uuid'] = self.source_task_id
        self.df = self.df.append(new_row, ignore_index=True)
        print(self.df)
    #todo Add more helper functions to make it easier to add multiple rows at a time
    def export(self):
        export_path = self.out_path.join('meaningful_title.csv')
        self.df.to_csv(export_path)
        return export_path



if __name__ == '__main__':
    #this is broken cause it's not a path data
    i =  IAA_task('../../test_data')
    i.add_row({'agreed_Answer':79, 'target_text': 'text was targeted'})
    i.export()
