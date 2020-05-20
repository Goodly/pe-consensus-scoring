import argparse

from IAA import calc_agreement_directory
from Dependency import eval_dependency
from export_tags import export_datahunt_tags

def iaa_only(directory, config_path, use_rep = False, repCSV = None, iaa_dir = None, schema_dir = None,
             scoring_dir = None, adjud_dir = None, threshold_func = 'raw_30'):
    """

    :param directory: the directory that holds all files from the tagworks datahunt export
    :param iaa_dir: the directory to output the raw IAA data to; if no input default is s_iaa_<directory>
    :param scoring_dir: directory to output data from every other stage of the scoring algorithm to; if no
        input default is scoring_<directory>
    :param repCSV: the csv that holds the rep score data
    :param use_rep: True if the scores should be computed using user rep scores; false otherwise
    :param: threshold_func: the threshold function being used to determine inter-annotator agreement; for a
        comprehensive test of all the threshold functions set this to 'all'; this will not work if an iaa_directory is
        specified
    :return: No explicit return.  Running will create two directories named by the inputs. the iaa_dir will house
        a csv output from the IAA algorithm.  The scoring_dir will house the csvs output from the dependency evaluation
        algorithm;
    """
    iaa_dir = calc_agreement_directory(
        directory,
        schema_dir,
        config_path,
        repCSV=repCSV,
        outDirectory=iaa_dir,
        useRep=use_rep,
        threshold_func=threshold_func
    )
    eval_dependency(directory, iaa_dir, schema_dir, out_dir=scoring_dir)
    export_datahunt_tags(scoring_dir, adjud_dir)
    return scoring_dir

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
        '-rf', '--rep-file',
        help='Filename to use for User Reputation scores file.')
    parser.add_argument(
        '-r', '--use_rep',
        help='True if we want to use reputation scores, false otherwise')
    parser.add_argument(
        '-tf', '--threshold_function',
        help= 'the threshold function used to check for inter annotator agreement'
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
    schema_dir = '../data/schemas/'
    rep_file = './UserRepScores.csv'
    use_rep = False
    threshold_function = 'raw_30'
    # output
    output_dir = '../data/datahunts/'
    scoring_dir = '../data/scoring/'
    adjud_dir = '../data/output_tags/'
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
    if args.rep_file:
        rep_file = args.rep_file
    if args.use_rep:
        use_rep = args.use_rep
    if args.threshold_function:
        threshold_function = args.threshold_function

    iaa_only(
        input_dir,
        config_path,
        use_rep = use_rep,
        repCSV = rep_file,
        iaa_dir = output_dir,
        schema_dir = schema_dir,
        scoring_dir = scoring_dir,
        adjud_dir = adjud_dir,
        threshold_func = threshold_function
    )
