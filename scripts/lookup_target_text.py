import csv

def lookup_target_text(article_sha256):
    viz_data_filename = "../data/out_viz/VisualizationData_{}.csv".format(article_sha256)
    article_filename = "../data/texts/{}.txt".format(article_sha256)
    with open(viz_data_filename, 'r') as csv_file, open(article_filename, 'r', encoding='utf-8') as text_file:
        text = text_file.read()
        reader = csv.DictReader(csv_file)
        for row in reader:
            try:
                start = int(row['Start'])
                end = int(row['End'])
            except ValueError:
                continue
            target = text[start:end]
            print('{},{},"{}"'.format(start, end, target))

article_sha256 = "4b537e0ed21179a29ed28da28057d338e67330ae12123ccceba6724f35bd68a4"
lookup_target_text(article_sha256)
