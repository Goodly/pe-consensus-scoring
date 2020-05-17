var articles = []
d3.csv("visData.csv", function(error, data) {
    for (var i = 0; i < Object.keys(data).length - 1; i++) {
        var article = data[i];
        articles.push(new Article(article["Title"], article["Author"], article["Date"], article["ID"], article["Article Link"], article["Visualization Link"], article["Plain Text"], article["Highlight Data"]));
    }
});

function matchString(quant, qual) {
    var outputArticles = [];
    var input = document.getElementById("textField").value;
    var re = new RegExp(input, 'gi');
    for (var i = 0; i < articles.length; i++) {
        //console.log(articles[i])
        if (articles[i].title.match(re) != null) {
            if (articles[i].publisher == qual) {
                outputArticles.push(articles[i]);
            } else if (qual == "number_sales") {
                if (articles[i].sales  > 100) {
                    outputArticles.push(articles[i]);
                }
            } else {
                outputArticles.push(articles[i]);
            }
        }
    }
    if (quant == "Alphabetical") {
        outputArticles.sort(alphabetize);
    } else if (quant == "Date Published") {
        outputArticles.sort(byDate);
    }
    if (outputArticles.length == 0) {
        document.getElementById("artlist").innerHTML = "No results found."
    }
    for (var i = 0; i < outputArticles.length; i++) {
        var titleLine = "<div class = 'articleTitle'> " + outputArticles[i].title + " </div>"
        var authorLine = "<div class = 'companyDate'> " + outputArticles[i].date + " </div>"
        var publisherLine = "<div class = 'author'> " + outputArticles[i].author + " </div>"
        var link = "<a href='" + outputArticles[i].visLink +"'>";
        var sunburst = "<svg id='sunburst" + outputArticles[i].id + "' class='svgContainer' viewBox='0 0 500 500'  preserveAspectRatio='xMidYMid meet'></svg>";
        document.getElementById("artlist").innerHTML =  document.getElementById("artlist").innerHTML +
            "<button id='category" + i + "' class='collapsible'> " + sunburst + titleLine + authorLine + publisherLine + "</button>" +
                "<div class='content'>" + link +
                    "<div class='homepage-section'>" +
                                "<div class='text article'>" +
                                    "<h2>Example Article with Public Editor labels</h2>" +
                                    "<p class='p-article' id='textArticle" + outputArticles[i].id + "'>" +
                                    "</p>" +
                                "</div>" +
                                "<div class='cred-score-container'>" +
                                    "<div class='sunburst'>" +
                                        "<svg id='chart" + outputArticles[i].id + "' viewBox='0 0 500 500'  preserveAspectRatio='xMidYMid meet'></svg>" +
                                    "</div>" +
                                "</div>" +
                            "</div>" +
                "<br></a></div>";
    }
    visualize(outputArticles);
    makeCollapsible();
}

function alphabetize(a,b) {
    if (a.title < b.title) {
        return -1;
    } else if (a.title > b.title) {
        return 1;
    } else {
        return 0;
    }
}

function byDate(a,b) {
    var aDate = parseInt(a.date.slice(0,4))*10000 + parseInt(a.date.slice(5,7))*100 + parseInt(a.date.slice(8,10))
    var bDate = parseInt(a.date.slice(0,4))*10000 + parseInt(a.date.slice(5,7))*100 + parseInt(a.date.slice(8,10))
    if (aDate < bDate) {
        return -1;
    } else if (aDate > bDate) {
        return 1;
    } else {
        return 0;
    }
}

function checkInput1() {
    document.getElementById("artlist").innerHTML = "";
    var input = document.getElementById("textField").value;
    var qual = document.getElementById("qualFilter").getElementsByTagName("*");
    var quant = document.getElementById("quantFilter").getElementsByTagName("*");
    var qualFilter;
    var quantFilter;
    
    for (var i = 0; i < qual.length; i++) {
        if (qual[i].selected) {
            
            qual = qual[i].value;
        }
    }
    for (var j = 0; j < quant.length; j++) {
        if (quant[j].selected) {
            
            quant = quant[j].value;
        }
    }
    matchString(quant, qual);

}

class Article {
    constructor(title, author, date, ID, articleLink, visLink, highlightData) {
        this.title = title;
        this.author = author;
        this.date = date;
        this.id = ID;
        this.articleLink = articleLink;
        this.visLink = visLink;
        this.highlightData = highlightData;
    }
}

function makeCollapsible() {
    var coll = document.getElementsByClassName("collapsible");
    var i;
    for (i = 0; i < coll.length; i++) {
      coll[i].addEventListener("click", function() {
        this.classList.toggle("active");
        var content = this.nextElementSibling;
        if (content.style.display === "block") {
          content.style.display = "none";
        } else {
          content.style.display = "block";
        }
      });
    }
}

function visualize(articles) {
    for (var i = 0; i < articles.length; i++) {
        curID = articles[i].id;
        runVisualization(curID);
        scoreArticle(curID);
    }
}



















