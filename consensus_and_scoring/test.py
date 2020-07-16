from Weighting import launch_Weighting
from pointAssignment import pointSort

scoring_dir = '../data/out_scoring/'
tua_dir ='../data/tags/'
reporting = True
input_dir = '../data/datahunts/'

weights = launch_Weighting(scoring_dir)
print("SORTING POINTS")
tuas, weights, tua_raw = pointSort(scoring_dir, input_dir=input_dir, weights=weights, tua_dir=tua_dir, reporting=reporting)