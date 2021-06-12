from string import Template
# Retrieve HTML template.

def insert_for_testing():
    with open("Visualization.html", "r") as f:
        html_source = f.read()
        html_template = Template(html_source)


    viz_data_filename = "vis_data.csv"
    article_filename = "article.txt"
    original_url = "https://pravda.ru/"
    context = {
        'DATA_CSV_URL': viz_data_filename,
        'ARTICLE_TEXT_URL': article_filename,
        'ORIGINAL_URL': original_url
    }
    # Merge template URLs into HTML file
    html_output = html_template.substitute(context)
    with open("Visualization.html", mode="w") as html_file:
        html_file.write(html_output)
        
def revert_for_pushing(data, article, originalurl):
    with open("Visualization.html", "r") as f:
        html_source = f.read()
        html_template = Template(html_source)


    viz_data_filename = "$DATA_CSV_URL";
    article_filename = "$ARTICLE_TEXT_URL";
    original_url = "$ORIGINAL_URL";
    context = {
        data: viz_data_filename,
        article: article_filename,
        originalurl: original_url
    }
    # Merge template URLs into HTML file
    html_output = html_template.substitute(context)
    with open("Visualization.html", mode="w") as html_file:
        html_file.write(html_output)