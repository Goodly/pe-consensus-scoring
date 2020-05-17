var articles = []
d3.csv("visData.csv", function(error, data) {
    var authorList = new Set([])
    for (var i = 0; i < Object.keys(data).length - 1; i++) {
        var article = data[i];
        articles.push(new Article(article["Title"], article["Author"], article["Date"], article["ID"], article["Article Link"], article["Visualization Link"], article["Plain Text"], article["Highlight Data"]));
        if (!authorList.has(article["Author"])) {
            document.getElementById("authorSelect").innerHTML += "<option value='" + article["Author"] + "'>" + article["Author"] + "</option>";
        }
        authorList.add(article["Author"]);
    }
    document.getElementById("toDate").setAttribute("max", today);
});

n =  new Date();
y = n.getFullYear();
m = n.getMonth() + 1;
d = n.getDate();
var today = y + "-" + m + "-" + d;

function matchString() {
    var outputArticles = [];
    var input = document.getElementById("textField").value;
    var author = document.getElementById("authorSelect").value;
    var toDate = document.getElementById("toDate");
    var fromDate = document.getElementById("fromDate");
    var re = new RegExp(input, 'gi');
    for (var i = 0; i < articles.length; i++) {
        if (articles[i].title.match(re) == null) {

            continue;
        }
        if (toDate.value) {
            if (toDate.value <= articles[i].date) {
                console.log("Here");
                continue;
            }
        }
        if (fromDate.value) {
            if (fromDate.value >= articles[i].date) {
                continue;
            }
        }
        if (author != "None") {
            if (author != articles[i].author) {
                continue;
            }
        }
        outputArticles.push(articles[i]);
    }

    if (outputArticles.length == 0) {
        document.getElementById("artlist").innerHTML = "No results found."
    }

    for (var i = 0; i < outputArticles.length; i++) {
        var titleEntry = "<td>" + outputArticles[i].title + "</td>";
        var dateEntry = "<td>" + outputArticles[i].date + "</td>";
        var authorEntry = "<td>" + outputArticles[i].author + "</td>";
        var link = "<a href='" + outputArticles[i].visLink +"'>";
        var articleEntry = "<td>" + outputArticles[i].id + "</td>";
        var sunburst = "<td><svg id='sunburst" + outputArticles[i].id + "' class='svgContainer' viewBox='0 0 500 500'  preserveAspectRatio='xMidYMid meet'></svg></td>";
        var hidden = "<td id='hidden" + outputArticles[i].id + "' hidden>" + "</td>";
        var modal = "<td><button id = 'category" + i + "'>" + outputArticles[i].id + "</button>" +
                    "<div id='modal" + i + "' class='modal'>" +
                        "<div id='modalcontent" + i + "' class='modal-content'>" +
                            "<span id='close" + i + "' class='close'>&times;</span>" +
                            "<div class='homepage-section'>" +
                                "<div id='myArticle" + outputArticles[i].id + "' class='text article'>" +
                                    "<h2>Example Article with Public Editor labels</h2>" +
                                    "<p class='p-article' id='textArticle" + outputArticles[i].id + "'></p>" +
                                "</div>" +
                                "<div class='cred-score-container'>" +
                                    "<div class='sunburst'>" +
                                        "<svg id='chart" + outputArticles[i].id + "' viewBox='0 0 500 500'  preserveAspectRatio='xMidYMid meet'></svg>" +
                                    "</div>" +
                                "</div>" +
                            "</div>" +
                        "</div>" +
                     "</div>" +
                     "</td>";
        var entry = "<tr>" + sunburst + titleEntry + dateEntry + authorEntry + modal + hidden + "</tr>";
        document.getElementById("artlist").innerHTML += entry;
    }
    visualize(outputArticles);
    modalize(outputArticles);
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
    document.getElementById("artlist").innerHTML = "" +
        "<tr><th onclick='sortTable(5)'> Sunburst</th><th onclick='sortTable(1)'> Title </th><th onclick='sortTable(2)'> Date </th><th onclick='sortTable(3)'> Author</th><th> Article </th><th hidden> Hidden </th></tr>";
    var input = document.getElementById("textField").value;
    matchString();
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

function visualize(articles) {
    for (var i = 0; i < articles.length; i++) {
        curID = articles[i].id;
        runVisualization(curID);
        scoreArticle(curID, i);
    }
}

function modalize2(i) {
        var btn = document.getElementById("category" + i);
        var modal = document.getElementById("modal" + i);
        var span = document.getElementsByClassName("close")[i];
        btn.addEventListener('click', function() {
          console.log("IM OPENING BLOCK " + i)
          modal.style.display = "block";
        });
        span.addEventListener('click', function() {
          modal.style.display = "none";
        });
        window.addEventListener('click', function(event) {
          if (event.target == modal) {
            modal.style.display = "none";
          }
        });
}

function modalize(articles) {
    for (var j = 0; j < articles.length; j++) {
        modalize2(j);
    }
}

function sortTable(n) {
  var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
  table = document.getElementById("artlist");
  switching = true;
  //Set the sorting direction to ascending:
  dir = "asc";
  /*Make a loop that will continue until
  no switching has been done:*/
  while (switching) {
    //start by saying: no switching is done:
    switching = false;
    rows = table.rows;
    /*Loop through all table rows (except the
    first, which contains table headers):*/
    for (i = 1; i < (rows.length - 1); i++) {
      //start by saying there should be no switching:
      shouldSwitch = false;
      /*Get the two elements you want to compare,
      one from current row and one from the next:*/
      x = rows[i].getElementsByTagName("TD")[n];
      y = rows[i + 1].getElementsByTagName("TD")[n];
      /*check if the two rows should switch place,
      based on the direction, asc or desc:*/
      if (dir == "asc") {
        if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
          //if so, mark as a switch and break the loop:
          shouldSwitch= true;
          break;
        }
      } else if (dir == "desc") {
        if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
          //if so, mark as a switch and break the loop:
          shouldSwitch = true;
          break;
        }
      }
    }
    if (shouldSwitch) {
      /*If a switch has been marked, make the switch
      and mark that a switch has been done:*/
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
      //Each time a switch is done, increase this count by 1:
      switchcount ++;
    } else {
      /*If no switching has been done AND the direction is "asc",
      set the direction to "desc" and run the while loop again.*/
      if (switchcount == 0 && dir == "asc") {
        dir = "desc";
        switching = true;
      }
    }
  }
}