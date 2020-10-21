import json
import pandas as pd
from pathlib import Path
import test_utils
import os

class dummy_data:
    #requires filetype is defined in higher scope
    def __init__(self,out_path = None, out_folder = None, source_task_id = None, article_num = None, schema=None):

        #for case where tempdir is passed in, outpath will be a path object, not a string
        if out_path!=None:
            try:
                self.out_path = out_path.mkdir()
            except Exception:
                self.out_path=out_path
        else:
            if out_folder == None:
                out_folder = self.config["test_dir"]
            self.out_path = Path(out_folder)
        if article_num == None:
            self.article_num = test_utils.make_number()
        else:
            self.article_num = article_num
        self.article_id = test_utils.make_sha256(article_num)

        # generate the id every row in this file will have
        if source_task_id == None:
            #this shouldn't be None unless its going to a tempdir
            self.source_task_id = test_utils.make_uuid()
        else:
            self.source_task_id = source_task_id
        self.base_row = test_utils.config['file_type'][self.filetype]
        self.cols = self.base_row.keys()
        self.df = pd.DataFrame(columns=self.cols)

    def add_row(self, params=None):
        new_row = self.base_row.copy()
        new_row['source_task_uuid'] = self.source_task_id
        new_row['article_sha256'] = self.article_id
        new_row['article_num'] = self.article_num
        keys = params.keys()
        if params != None:
            for p in keys:
                if p not in self.cols:
                    raise Exception("Column name entered <" + p + "> isn't a column name in the "+self.filetype+" table")
                if p == 'namespace':
                    schema_sha256 = test_utils.sha256_from_namespace(params[p])
                    new_row['schema_sha256'] = schema_sha256
                new_row[p] = params[p]
        new_row = self.fill_in_logic(new_row, params)

            # todo: logically fill in other rows; ie given schema name fill in schema_id, given answer/schema fill in answer_uuid
        self.df = self.df.append(new_row, ignore_index=True)
        # todo Add more helper functions to make it easier to add multiple rows at a time

    def fill_in_logic(self, row, params):
        return row


    def set_row(self, column, value):
        self.df[column] = value
    def set_out_name(self, filetype, source_task_id):
        return filetype+'_'+source_task_id+'.csv'
    def export(self):
        out_name = self.set_out_name (self.filetype, self.source_task_id)
        export_path = os.path.join(self.out_path,out_name)

        print(export_path)
        self.df.to_csv(export_path, encoding='utf-8')
        return export_path
