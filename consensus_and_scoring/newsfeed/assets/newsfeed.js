window.addEventListener('load', (event) => {
    const visPromise = readVisData();
    visPromise.then(function() {
        generateAndMove();

    });

});


var listofarticles = [];

function readVisData() {
    return $.get("visData.json").done(function(data) {
        console.log(data);
        for (var i = 0; i < Object.keys(data).length; i++) {
            var article = data[i];
            var articleEntry = new ArticleData(article["Title"], article["Author"], article["Date"], article["ID"], article["Article Link"], article["Visualization Link"], article["Plain Text"], article["Highlight Data"]);
            //articleEntry.setCredibilityScore();

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


function generateList() {
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

    //Only show the top X results
    var showLimit = Math.max(sortedArticles.length, document.getElementById("showLimit").value);
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
      console.log(listofarticles);
        if (order == "high") {

            listofarticles.sort((a, b) => (a.credibilityScore < b.credibilityScore) ? 1 : -1)
        } else {
            listofarticles.sort((a, b) => (a.credibilityScore > b.credibilityScore) ? 1 : -1)
        }
    }
    return listofarticles;
}

function generateAndMove() {

  $.get("#showLimit").done(function(){
    const generatePromise = new Promise(function() {
       generateList();
    });

      moveHallmarks();



  } );



}

function generateEntry(entry) {

    var previewPromise = $.get(entry.plainText).done(function(data) {
      var totalText = data.toString()
      var previewText = totalText.substring(0, 200);

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

      var articleEntry = "<div id='" + entry.id + "' class='row'>" +
                            "<div class='col-2 date'>" + entry.date + "</div>" +
                            "<div class='col-6'>" +
                                "<a class='hyperlink' href='" + entry.visLink + "' target='_blank'> <h3>" + entry.title + "</h3></a>" +
                                "<p class='articleText'>" + previewText + "</p>" +
                                "<p class='author'>" + entry.author + "</p>" +
                            "</div>" +
                            "<div class='cred-score-container col-4'>" +
                                "<div class='sunburst'>" +
                                    "<svg id='sunburst" + entry.id + "' viewBox='0 0 200 200'  preserveAspectRatio='xMidYMid meet'></svg>" +
                                "</div>" +
                            "</div>" +
                       "</div>" +
                       "<hr>";
      document.getElementById("articleList").innerHTML += articleEntry;
      if (document.querySelector("svg[articleID='" + entry.id +"']") != null) {
          document.querySelector("svg[articleID='" + entry.id +"']").remove();
      }
      hallmark(entry.highlightData, entry.id);


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
