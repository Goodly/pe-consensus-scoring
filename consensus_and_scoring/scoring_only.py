import argparse


from Dependency import  eval_dependency
from Weighting import  launch_Weighting
from pointAssignment import pointSort
from holistic_eval import eval_triage_scoring
from Separator import  splitcsv

def scoring_only(directory, iaa_dir, schema_dir, scoring_dir, viz_dir, tua_dir, threshold_func, reporting = False):
    eval_dependency(directory, iaa_dir, schema_dir, out_dir=scoring_dir)
    print("WEIGHTING")
    weights = launch_Weighting(scoring_dir, reporting=reporting)
    print("SORTING POINTS")
    tuas, weights, tua_raw = pointSort(scoring_dir, input_dir=directory, weights=weights, tua_dir=tua_dir,
                                       reporting=reporting)
    points = eval_triage_scoring(tua_raw, weights, scoring_dir, threshold_func, reporting=reporting)
    print("SPLITTING")
    if viz_dir == None:
        x = directory.rfind("/")
        x += 1
        viz_dir = '../../visualization_' + directory[x:]
    splitcsv(scoring_dir, pointsFile=points, viz_dir=viz_dir, reporting=reporting)

def load_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i', '--input-dir',
        help='Directory containing DataHuntHighlights DataHuntAnswers, '
             'and Schema .csv files.')
    parser.add_argument(
        '-t', '--schema-dir',
        help='path to directory with schemas CSVs named by SHA-256 of source')
    parser.add_argument(
        '-o', '--output-dir',
        help='Pathname to use for IAA output directory.')
    parser.add_argument(
        '-s', '--scoring-dir',
        help='Pathname to use for output files for scoring of articles.')
    parser.add_argument(
        '-tf', '--threshold_function',
        help= 'the threshold function used to check for inter annotator agreement'
    )
    parser.add_argument(
        '-u', '--tua_dir',
        help= 'input directory for TUA data'
    )
    parser.add_argument(
        '-v', '--viz_dir',
        help= 'output directory for visulizations'
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = load_args()
    # input
    config_path = './config/'
    input_dir = '../data/datahunts/'
    texts_dir = '../data/texts/'
    #metadata_dir = '../data/metadata/'
    tua_dir = '../data/focus_tags/'
    schema_dir = '../data/schemas/'
    # output
    output_dir = '../data/out_adjudicated_iaa/'
    scoring_dir = '../data/out_scoring/'
    viz_dir = '../data/out_viz/'
    threshold_function = 'raw_30'
    if args.input_dir:
        input_dir = args.input_dir
    if args.schema_dir:
        schema_dir = args.schema_dir
    if args.output_dir:
        output_dir = args.output_dir
    if args.scoring_dir:
        scoring_dir = args.scoring_dir
    if args.viz_dir:
        viz_dir = args.viz_dir
    if args.threshold_function:
        threshold_function = args.threshold_function
    if args.tua_dir:
        tua_dir = args.tua_dir


    scoring_only(
        input_dir,
        output_dir,
        schema_dir,
        scoring_dir,
        viz_dir,
        tua_dir,
        threshold_function,
        reporting = True
    )

