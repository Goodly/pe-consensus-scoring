var ARTICLES_PER_LOAD = 10;
var TOTAL_ARTICLES_DISPLAYED = 10;
var LIST_OF_ARTICLES = [];
var NUM_ARTICLES = -1;
var LOADING = false;


async function loadInitial() {
    $.get("visData.json").done((data) => {
        readVisData(data, 0).then(() => {
            var articleIndex = 0;
            console.log(LIST_OF_ARTICLES);
            LIST_OF_ARTICLES.forEach(function (entry, _) {
                $.get(entry.plainText).done((data) => {
                    entry.totalText = data;
                    const entryPromise = generateEntry(entry);
                    entryPromise.then(() => {
                        moveHallmark(articleIndex);
                        articleIndex += 1;
                    });
                });
            })
        });    
    });
}

window.addEventListener('load', (event) => {
    loadInitial();
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
            readVisData(data, articleIndex).then(() => {
                LIST_OF_ARTICLES.slice(articleIndex, TOTAL_ARTICLES_DISPLAYED).forEach(function (entry, _) {
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
        });          
    }
}

function readVisData(data, start) {
    promises = [];
    NUM_ARTICLES = Object.keys(data).length;
    data.slice(start, start+ARTICLES_PER_LOAD).forEach(function (entry, _) {
        var article = entry;
        var triage_path = "/visualizations/" + article["article_sha256"].substring(0, 32) +"/triager_data.csv";
        // await getScore(article['high'])
        
        promises.push(
            new Promise(resolve => {
                d3.csv(triage_path, function(error, _) {
                    if (error) {    
                        console.log(error);
                        console.log('couldnt find articles triage data');
                        resolve();
                    } else {
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
                        resolve();
                    }
                });
            }));
    });
    return Promise.all(promises);
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


async function generateEntry(entry) {
    var totalText = entry.totalText;
    var previewText = totalText.substring(0, 200);
      // Empty string checks
    if (entry.title == "") {
        const regexTitle = /(Title:).+/
        const matches = totalText.match(regexTitle);
        if (matches === null) {
            const title_string = totalText.split(/\s+/).slice(0,5).join(' ');
            entry.title = title_string + '...';
        } else if (matches.length != 0) {
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