var ARTICLES_PER_LOAD = 2;
var TOTAL_ARTICLES_DISPLAYED = 2;
var LIST_OF_ARTICLES = [];
var NUM_ARTICLES = -1;
var LOADING = false;

window.addEventListener('load', (event) => {
    $.get("visData.json").done((data) => {
        readVisData(data, 0);
        var entry;
        console.log(LIST_OF_ARTICLES);
        var articleIndex = 0;
        LIST_OF_ARTICLES.forEach(function (entry, _) {
            $.get(entry.plainText).done((data) => {
                entry.totalText = data;
                const entryPromise = generateEntry(entry);
                entryPromise.then(() => {
                    moveHallmark(articleIndex);
                    articleIndex += 1;
                })
                // moveHallmark(articleIndex);
            })
        })
            
    });
        // generateAndMove("all;");

});



window.onscroll = function() {
    
    if (window.innerHeight + window.pageYOffset - 39 >= document.body.offsetHeight) {
        // you're at the bottom of the page
        if (TOTAL_ARTICLES_DISPLAYED < NUM_ARTICLES) {
            if (!LOADING) {
                $('.loader').css('display', 'block');
                // console.log("loadArticle called");
                LOADING = true;
                setTimeout(loadArticlesOnScroll, 2000);
            }
        }
    } else if (window.innerHeight + window.pageYOffset + 100 < document.body.offsetHeight) {
        LOADING = false;
        $('.loader').css('display', 'none');
    }
};

function loadArticlesOnScroll() {
    // console.log('Scroll height', window.innerHeight + window.pageYOffset);
    // console.log('document height', document.body.offsetHeight);
    if (window.innerHeight + window.pageYOffset + 90 >= document.body.offsetHeight) {
        const newArticleStartIndex = TOTAL_ARTICLES_DISPLAYED;
        TOTAL_ARTICLES_DISPLAYED = TOTAL_ARTICLES_DISPLAYED + ARTICLES_PER_LOAD;
        var articleIndex = newArticleStartIndex;
        $.get("visData.json").done((data) => {
            $('.loader').css('display', 'none');
            readVisData(data, articleIndex);
            LIST_OF_ARTICLES.slice(articleIndex, TOTAL_ARTICLES_DISPLAYED).forEach(function (entry, _) {
                console.log(entry.plainText);
                $.get(entry.plainText).done((data) => {
                    entry.totalText = data;
                    const entryPromise = generateEntry(entry);
                    entryPromise.then(() => {
                        moveHallmark(articleIndex);
                        articleIndex += 1;
                    });
                    // moveHallmark(articleIndex);
                });
            });
            LOADING = false;

        });          
    }
}





function readVisData(data, start) {
    NUM_ARTICLES = Object.keys(data).length;
    // console.log(TOTAL_ARTICLES_DISPLAYED);
    for (var i = start; i < TOTAL_ARTICLES_DISPLAYED; i++) {
        var article = data[i];
        var triage_path = "/visualizations/" + article["article_sha256"].substring(0, 32) +"/triager_data.csv";
        // await getScore(article['high'])
        var articleEntry = new ArticleData(
                                    article["Title"], 
                                    article["Author"], 
                                    article["Date"], 
                                    article["ID"],
                                    article["Visualization Link"], 
                                    article["Plain Text"],
                                    article["Highlight Data"], 
                                    triage_path,
                                    article["article_sha256"]);

        LIST_OF_ARTICLES.push(articleEntry);
    }
}

// async function readVisData() {
//     $.get("visData.json").done(function(data) {
        
//         // console.log(LIST_OF_ARTICLES)
//     });
// }

// function setScores() {

//     var articleObject;
//     for (articleObject of LIST_OF_ARTICLES) {
//         console.log(artSVG.getAttribute("score"));
//         var artSVG = document.querySelector("svg[articleID='" + articleObject.id + "'");
//         articleObject.credibilityScore = parseInt(artSVG.getAttribute("score"));
//     }
// }


function generateList(limit) {
    // Collect values from the HTML
    //Sort by... Most Recent, Alphabetical, Credibility Score (High to Low & Low to High)
    var sortOptions = document.getElementById("sortByList");
    var sortBy = sortOptions.options[sortOptions.selectedIndex].value;
    var orderOptions = document.getElementById("order");
    var order = orderOptions.options[orderOptions.selectedIndex].value;

    //sort
    sortArticles(sortBy, order);
    //Filter by tags (Needs additional information)

    //Only show the top 20 results
    if (limit == 20) {
      var showLimit = 20;
    } else {
      var showLimit = LIST_OF_ARTICLES.length;
    }
    LIST_OF_ARTICLES = LIST_OF_ARTICLES.slice(0, showLimit);
    document.getElementById("articleList").innerHTML = "";

    for (var i = 0; i < LIST_OF_ARTICLES.length; i++) {
        generateEntry(LIST_OF_ARTICLES[i]);
    }
    return false;
}

// As the hallmarks are generated and placed, we don't want the user to scroll and
// mess up the positioning.
async function scrollPause() {
  window.scrollTo(0, 0);
  $('body').addClass('stop-scrolling');
}

function scrollContinue() {
  $('body').removeClass('stop-scrolling');
}

function sortArticles(sortBy, order) {
    if (sortBy == "title") {
        if (order == "revAlpha") {
            LIST_OF_ARTICLES = LIST_OF_ARTICLES.sort((a, b) => (a.title < b.title) ? 1 : -1)
        } else {
            LIST_OF_ARTICLES = LIST_OF_ARTICLES.sort((a, b) => (a.title >= b.title) ? 1 : -1)
        }
    } else if (sortBy == "date") {
        if (order == "older") {
            LIST_OF_ARTICLES = LIST_OF_ARTICLES.sort((a, b) => (Date.parse(a.date) > Date.parse(b.date)) ? 1 : -1)
        } else {
            LIST_OF_ARTICLES = LIST_OF_ARTICLES.sort((a, b) => (Date.parse(a.date) < Date.parse(b.date)) ? 1 : -1)
        }
    } else {
        if (order == "high") {
            
            LIST_OF_ARTICLES = LIST_OF_ARTICLES.sort((a, b) => (a.credibilityScore < b.credibilityScore) ? 1 : -1)
        } else {
            LIST_OF_ARTICLES = LIST_OF_ARTICLES.sort((a, b) => (a.credibilityScore >= b.credibilityScore) ? 1 : -1)
        }
    }
}

async function generateAndMove(limit) {
  await removeHallmarks();
  await scrollPause();
  await generateList(limit);

  await moveHallmarks();
  scrollContinue();
}


async function generateEntry(entry) {
    var totalText = entry.totalText;
    var previewText = totalText.substring(0, 200);
      // Empty string checks
    if (entry.title == "") {
        const regexTitle = /(Title:).+/
        const matches = totalText.match(regexTitle);
        if (matches.length != 0) {
            const title_string = matches[0].substring(7, matches[0].length); //Slice out the 'Title: '
            entry.title = title_string;
        }
    }
    var articleEntry = "<div id='" + entry.sha256 + "' class='row'>" +
                        "<div class='col-2 date'>" + entry.date + "</div>" +
                        "<div class='col-6'>" +
                            "<a class='hyperlink' href='" + entry.visLink + "' target='_blank'> <h3>" + entry.title + "</h3></a>" +
                            "<p class='articleText'>" + previewText + "</p>" +
                            "<p class='author'>" + entry.author + "</p>" +
                        "</div>" +
                        "<div class='cred-score-container col-4'>" +
                            "<div class='sunburst'>" +
                                "<svg id='sunburst_" + entry.sha256 + "' viewBox= '0 0 200 200'></svg>" +
                            "</div>" +
                        "</div>" +
                    "</div>" +
                    "<hr>";
    document.getElementById("articleList").innerHTML += articleEntry;
    if (document.querySelector("svg[articleID='" + entry.id +"']") != null) {
        document.querySelector("svg[articleID='" + entry.id +"']").remove();
    }
    return hallmark(entry);
}

function csvJSON(csv){
    var lines=csv.split("\n");
    var result = [];
    var headers=lines[0].split(",");
    for(var i=1;i<lines.length;i++){
        var obj = {};
        var currentline=lines[i].split(",");
    for(var j=0;j<headers.length;j++){
        obj[headers[j]] = currentline[j];
    }
    result.push(obj);
  }
  //return result; //JavaScript object
  return result
}


// Reformat date to the style MONTH - DAY - YEAR (September 16, 2017)
// @param date : string containing date info.
function reformatDate(date_string) {
  var options = { month: 'long'};

  const date_object = new Date(date_string);

  const day = date_object.getDate();
  const year = date_object.getFullYear();
  const month = new Intl.DateTimeFormat('en-US', options).format(date_object)
  return month + " " + day + ", " + year;
}