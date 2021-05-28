from import_tags import import_tags
from scoring_only import scoring_only
from IAA import calc_agreement_directory
from dataV3 import make_directory
from MergeHighlighter import merge
import argparse

def post_adjudicator_master(tags_dir, schema_dir, new_s_iaa_dir, iaa_temp_dir, input_dir, scoring_dir, viz_dir,
                            tua_dir, text_dir, config_path, concat_tua_dir, threshold_func):
    iaa_dir = calc_agreement_directory(input_dir, schema_dir, config_path, text_dir, repCSV=None,  outDirectory = iaa_temp_dir,
                             useRep = False, threshold_func = threshold_func)
    import_tags(iaa_dir, tags_dir, schema_dir, new_s_iaa_dir)
    scoring_only(input_dir, new_s_iaa_dir, schema_dir, scoring_dir, viz_dir, tua_dir, threshold_func)
    merge(tua_dir, concat_tua_dir)


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
            help='the threshold function used to check for inter annotator agreement'
        )
        parser.add_argument(
            '-u', '--tua_dir',
            help='input directory for TUA data'
        )
        parser.add_argument(
            '-v', '--viz_dir',
            help='output directory for visualizations'
        )
        return parser.parse_args()

if __name__ == '__main__':
        args = load_args()
        # input
        config_path = './config/'
        input_dir = '../test_output/publish_p4-a530712123/datahunts'
        texts_dir = '../test_output/publish_p4-a530712123/texts/'
        adjudication_dir = '../test_output/publish_p4-a530712123/tags/'
        # metadata_dir = '../data/metadata/'
        tua_dir = '../test_output/publish_p4-a530712123/focus_tags/'
        schema_dir = '../test_output/publish_p4-a530712123/schemas/'
        #output data
        iaa_temp_dir = make_directory('../test_output/publish_p4-a530712123/output_temp_iaa/')
        adjudicated_dir = make_directory('../test_output/publish_p4-a530712123/output_adjudicated_iaa/')
        scoring_dir = make_directory('../test_output/publish_p4-a530712123/output_scoring/')
        viz_dir = make_directory('../test_output/publish_p4-a530712123/output_viz/')
        concat_tua_dir = make_directory('../test_output/publish_p4-a530712123/output_concat_tags/')
        threshold_function = 'raw_50'
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
        post_adjudicator_master(adjudication_dir, schema_dir, adjudicated_dir, iaa_temp_dir, input_dir, scoring_dir, viz_dir,
                                tua_dir, texts_dir, config_path, concat_tua_dir, threshold_function)


