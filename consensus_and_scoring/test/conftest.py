from IAA_task import IAA_task
import pytest


@pytest.fixture
def make_task_demo(tmpdir):
    print(tmpdir)
    task = IAA_task(tmpdir)
    task.add_row({'agreed_Answer': 79, 'target_text': 'text was targeted'})
    task.set_agreement_score(.2)
    out_path = task.export()
    return out_path

#todo Add fixture for mass-making multiple tasks at once
#this can be passed into many other fixtures
