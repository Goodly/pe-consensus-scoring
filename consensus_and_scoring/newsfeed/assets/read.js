var articles = [];
d3.csv("test.csv", function(error, data) {
    for (var i = 0; i < Object.keys(data).length - 1; i++) {
        var article = data[i];
        articles.push(new Article(article["Title"], article["Author"], article["Publisher"], article["Date Published"], article["Sales in Millions"], article["Synopsis"], article["Link"]));
    }
    console.log(articles)
});

function matchString(quant, qual) {
    var outputArticles = [];
    var input = document.getElementById("textField").value;
    var re = new RegExp(input, 'gi');
    for (var i = 0; i < articles.length; i++) {
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
    for (var i = 0; i < outputArticles.length; i++) {
        var titleLine = "<div class = 'articleTitle'> " + outputArticles[i].title + " </div>"
        var authorLine = "<div class = 'companyDate'> " + outputArticles[i].date + " </div>"
        var publisherLine = "<div class = 'author'> " + outputArticles[i].author + " </div>"
        var icon = "<img src='icon.png' style='float:left; margin-left: 6px; margin-top: 6px; width:70px; height:70px;' >"
        var link = "<a href='" + outputArticles[i].link +"'>";
        document.getElementById("artlist").innerHTML =  document.getElementById("artlist").innerHTML +
            "<button id='category" + i + "' class='collapsible'> " + icon + titleLine + authorLine + publisherLine + "</button><div class='content'>" +link + icon + titleLine + authorLine + publisherLine + "<br></a></div>";
    }
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
    if (a.date < b.date) {
        return -1;
    } else if (a.date > b.date) {
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
    constructor(title, author, publisher, date, sales, synopsis, link) {
        this.title = title;
        this.author = author;
        this.publisher = publisher;
        this.date = date;
        //this.date = parseInt(date.slice(0,4));
        this.sales = parseInt(sales, 10);
        this.synopsis = synopsis;
        this.link = link;
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



















