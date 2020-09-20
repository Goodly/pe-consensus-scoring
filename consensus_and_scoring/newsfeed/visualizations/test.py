import json
import csv
import os
import glob
import pandas as pd

with open("../visData.json") as f:
    data = json.load(f)


#print(data)
file_names =  []

for item in data:
    file_names.append(data['Highlight Data'])

combined_csv = pd.concat([pd.read_csv(f) for f in file_names ])

combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')

# with open('..' + file_names[0], newline = '') as csvfile:
#     spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#     for row in spamreader:
#         print(', '.join(row))
