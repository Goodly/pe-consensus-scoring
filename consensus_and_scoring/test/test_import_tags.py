import pytest
import pandas as pd

def test_demo(make_task_demo):
    print(make_task_demo)
    df = pd.read_csv(make_task_demo)
    assert len(df.index) == 1
    assert len(df.index) == 2

#todo Come up with other testing util funcs for comparing inputs/outputs