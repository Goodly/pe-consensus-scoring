import pandas as pd
import os

import test_utils
from filegen_utils import *
from Weighting import *


def test_weighting_sample(config):
    out_path = test_utils.make_test_directory(config, 'weighting_iaa_sample_test')
    weight_out_folder = test_utils.make_test_directory(config, 'out_weighting_iaa_sample_test')

    iaa = dep_iaa(out_folder=out_path, source_task_id='weightsampletests')
    #-.5 points--from the weight key in config folder and the agreement_score
    iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": 2, "question_Number": 4, "agreement_score":1})
    #-2 points from ./config/weight_key and agreement score
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 1, "question_Number": 8, "agreement_score": .5})
    # +1.5 points from ./config/weight_key and agreement score
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 1, "question_Number": 12, "agreement_score": .75})
    fin_path = iaa.export()
    #weighting will output the actual pandas dataframe instead of the directory
    #if you look into the Weighting.py file, you can see the paths to
    weighting_out =  launch_Weighting(out_path, weight_out_folder)
    points = weighting_out['agreement_adjusted_points']
    tot = points.sum()
    print(weighting_out)
    assert tot == -1
    assert len(weighting_out.index) == 3