from Weighting import launch_Weighting
from pointAssignment import pointSort
from Separator import indicesToStartEnd
import os
import pandas as pd
scoring_dir = '../test_data/pa_dep_input/'
tua_dir ='../test_data/pa_tua_input/'
reporting = True
input_dir = None

weights = launch_Weighting(scoring_dir)
print("SORTING POINTS")
print(scoring_dir, input_dir, '\n',weights.columns, '\n', tua_dir, reporting)
tuas, weights, tua_raw = pointSort(scoring_dir, input_dir=None, weights=weights, tua_dir=tua_dir, reporting=reporting)

arr = [1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080, 1081, 1082, 1083, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095, 1096, 1097, 1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105]
o = indicesToStartEnd(arr)
print(o)

arr = []
o = indicesToStartEnd(arr)
print(o)

def join_csvs_in_directory(in_directory, out_directory= None):
    in_files = []
    for root, dir, files in os.walk(in_directory):
        for file in files:
            in_files.append(in_directory + '/' + file)
    temp_dfs = []
    for i in range(len(in_files)):
        temp_dfs.append(pd.read_csv(in_files[i]))
    files = pd.concat(temp_dfs)
    if out_directory == None:
        out_directory = in_directory
    out_path = os.path.join(out_directory, 'all_csvs_stacked.csv')
    print("joining outputting to ", out_path)
    files.to_csv(out_path)

if __name__ == '__main__':
    in_directory = '../data/adj_tags/'
    out_directory = '../data/out_viz/'
    join_csvs_in_directory(in_directory, out_directory)