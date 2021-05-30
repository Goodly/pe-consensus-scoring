window.addEventListener('load', (event) => {
    SVG_IDS = [];
    const visPromise = readVisData();
    visPromise.then(function() {
        generateAndMove(20);

    });

});


// On showLimit selection change, regenerate hallmarks and move them into place
//

$(document).on('change','#showLimit',function(e){
    var limit = this.options[e.target.selectedIndex].text;
    generateAndMove(limit);
});

var listofarticles = [];

function readVisData() {
    return $.get("visData.json").done(function(data) {
        for (var i = 0; i < Object.keys(data).length; i++) {
            var article = data[i];
            var triage_path = "/visualizations/" + article["article_sha256"].substring(0, 32) +"/triager_data.csv";
            var articleEntry = new ArticleData(article["Title"], article["Author"], article["Date"], article["ID"],
                                              article["Article Link"], article["Visualization Link"], article["Plain Text"],
                                              article["Highlight Data"], triage_path, article["article_sha256"]);

            listofarticles.push(articleEntry);
        }
    });
}

function setScores() {

    var articleObject;
    for (articleObject of listofarticles) {
        var artSVG = document.querySelector("svg[articleID='" + articleObject.id + "'");
        articleObject.credibilityScore = parseInt(artSVG.getAttribute("score"));
    }
}


async function generateList(limit) {
    // Collect values from the HTML
    //Sort by... Most Recent, Alphabetical, Credibility Score (High to Low & Low to High)
    var sortOptions = document.getElementById("sortByList");
    var sortBy = sortOptions.options[sortOptions.selectedIndex].value;
    var orderOptions = document.getElementById("order")
    var order = orderOptions.options[orderOptions.selectedIndex].value;
    search = document.getElementById("searchtext").value;

    //search
    var searchedArticles = unlimitedSearchWorks(search, listofarticles);

    //sort
    var sortedArticles = sortArticles(searchedArticles, sortBy, order);
    //Filter by tags (Needs additional information)

    //Only show the top 20 results
    if (limit == 20) {
      var showLimit = 20;
    } else {
      var showLimit = sortedArticles.length;
    }
    sortedArticles = sortedArticles.slice(0, showLimit);
    document.getElementById("articleList").innerHTML = "";

    for (var i = 0; i < sortedArticles.length; i++) {
        generateEntry(sortedArticles[i]);
    }
    return false;
}

function unlimitedSearchWorks(query, listofarticles) {
    output = []
    var re = new RegExp(search, 'gi');
    for (var i = 0; i < listofarticles.length; i++) {
        if (listofarticles[i].title.match(re) != null) {
            output.push(listofarticles[i])
        }
    }
    return output
}

function sortArticles(listofarticles, sortBy, order) {
    if (sortBy == "title") {
        if (order == "revAlpha") {
            listofarticles.sort((a, b) => (a.title < b.title) ? 1 : -1)
        } else {
            listofarticles.sort((a, b) => (a.title > b.title) ? 1 : -1)
        }
    } else if (sortBy == "date") {
        if (order == "older") {
            listofarticles.sort((a, b) => (Date.parse(a.date) > Date.parse(b.date)) ? 1 : -1)
        } else {
            listofarticles.sort((a, b) => (Date.parse(a.date) < Date.parse(b.date)) ? 1 : -1)
        }
    } else {
        if (order == "high") {

            listofarticles.sort((a, b) => (a.credibilityScore < b.credibilityScore) ? 1 : -1)
        } else {
            listofarticles.sort((a, b) => (a.credibilityScore > b.credibilityScore) ? 1 : -1)
        }
    }
    return listofarticles;
}

async function generateAndMove(limit) {
  await removeHallmarks();
  await generateList(limit);

  moveHallmarks();
}


function generateEntry(entry) {

    var previewPromise = $.get(entry.plainText).done(function(data) {
      var totalText = data.toString()
      var previewText = totalText.substring(0, 200);
      // Empty string checks
      if (entry.title == "") {
        const regexTitle = /(Title:).+/
        const matches = totalText.match(regexTitle);
        const title_string = matches[0].substring(7, matches[0].length); //Slice out the 'Title: '
        entry.title = title_string;
      }

      if (entry.date == "") {
        const regexDate = /(Date.+:)(.+)/
        matches = regexDate.exec(totalText);
        const date_string = matches[2]; // Get the last match, which is the date;
        entry.date = date_string;
      }

      entry.date = reformatDate(entry.date);

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
      console.log(document.getElementById(entry.sha256));
      if (document.querySelector("svg[articleID='" + entry.id +"']") != null) {
          document.querySelector("svg[articleID='" + entry.id +"']").remove();
      }
      hallmark(entry.highlightData, entry.triageData, entry.sha256);


    });

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
