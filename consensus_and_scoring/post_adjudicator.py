from import_tags import import_tags
from scoring_only import scoring_only
from IAA import calc_agreement_directory
import argparse

def post_adjudicator_master(tags_dir, schema_dir, new_s_iaa_dir, input_dir, scoring_dir, viz_dir,
                            tua_dir, text_dir, threshold_func, config_path):
    iaa_dir = calc_agreement_directory(input_dir, schema_dir, config_path, text_dir, repCSV=None,  outDirectory = '../data/temp_iaa/',
                             useRep = False, threshold_func = 'raw_30')
    import_tags(iaa_dir, tags_dir, schema_dir, new_s_iaa_dir)
    scoring_only(input_dir, new_s_iaa_dir, schema_dir, scoring_dir, viz_dir, tua_dir, threshold_func)


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
            help='output directory for visulizations'
        )
        return parser.parse_args()

if __name__ == '__main__':
        args = load_args()
        # input
        config_path = './config/'
        input_dir = '../data/datahunts/'
        texts_dir = '../data/texts/'
        # metadata_dir = '../data/metadata/'
        tua_dir = '../data/tags/'
        iaa_dir = '../data/raw_iaa/'
        schema_dir = '../data/schemas/'
        adjudication_dir = '../data/output_tags/'
        adjudicated_dir = '../data/adjudicated_iaa/'
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
        post_adjudicator_master(adjudication_dir, schema_dir, adjudicated_dir, input_dir, scoring_dir, viz_dir,
                            tua_dir, texts_dir, threshold_function, config_path)


