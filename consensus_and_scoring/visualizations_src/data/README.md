### Overview

The two scripts that need to be integrated are datahunt_aggregate.py and form_aggregate.py

## datahunt_aggregate.py
The main function is create_eta_datahunt. It takes a path to a folder called "datahunt" that contains all the relevant datahunts. It needs the sha256
of the article we are trying to visualize. We also take in a target_dir, which by default writes the file to the parent directory. We also expect the 
weight_key.csv to be in the working directory.

## form_aggregate.py
The only function here is simple_data_from_raw_data. It takes in the sha256 of the article you are trying to visualize, the target_dir to write the output csv.
By default, writes it in the parent directory of data--the visualizations_src directory. It takes in another parameter, path, the path to the file 

Covid_Form1.0.adjudicated-2020-10-04T2314-Tags.csv

by default.