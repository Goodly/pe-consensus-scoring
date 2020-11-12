import pandas as pd
import numpy as np

def AgreementScore(iaaData, schemaPath):
    print("AGREEMENT SCORING TIME!!!")
    print("OLD AGREEMENT SCORES:")
    print(iaaData['agreement_score'])
    #TODO: AGREEMENT SCORE CHANGES HERE
    #iaaData['agreement_score'] = np.zeros(3)
    print("NEW AGREEMENT SCORES:")
    print(iaaData['agreement_score'])
    return iaaData
