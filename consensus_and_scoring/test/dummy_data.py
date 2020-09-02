import json
import pandas as pd

class dummy_data:
    #todo add other params so can be more useful
    def __init__(self, filetype, out_path, source_task_id = None):
        # out_path is a path object
        self.out_path = out_path.mkdir('test_'+filetype)

        # generate the id every row in this file will have
        self.source_task_id = 'xuaixs7a8xnia89'
        # todo have similar process for article_uuid, etc
        with open('test_config.json') as json_file:
            data = json.load(json_file)

        self.base_row = data['file_type'][filetype]
        self.cols = self.base_row.keys()
        self.df = pd.DataFrame(columns=self.cols)

        # todo add helper functions to make the defaults more defaulty

    def add_row(self, params=None):
        new_row = self.base_row
        if params == None or 'source_task_uuid' not in params.keys():
            new_row['source_task_uuid'] = self.source_task_id
        if params != None:
            for p in params.keys():
                if p not in self.cols:
                    raise Exception("Column name entered" + p + " isn't a column name in the IAA table")
                new_row[p] = params[p]
                # todo: logically fill in other rows; ie given schema name fill in schema_id, given answer/schema fill in answer_uuid
        print(new_row)
        self.df = self.df.append(new_row, ignore_index=True)
        print(self.df)
        # todo Add more helper functions to make it easier to add multiple rows at a time

    def set_row(self, column, value):
        self.df[column] = value

    def export(self):
        export_path = self.out_path.join('meaningful_title.csv')
        self.df.to_csv(export_path)
        return export_path
