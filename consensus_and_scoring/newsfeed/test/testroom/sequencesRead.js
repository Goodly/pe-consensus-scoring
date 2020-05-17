//This section switches out the existing article for one from a .txt file.
function scoreArticle(articleNumber) {
    //Use this to control which csv and txt are being used.
    d3.text(articleNumber + "SSSArticle.txt", function(text) {
        document.getElementById("textArticle").innerHTML = text.toString();
    });

    //This section parses the CSV file into a JSON.
    d3.csv("VisualizationData_" + articleNumber + ".csv", function(error, data) {
    if (error) throw error;
    var articles = buildHierarchy(data);
    console.log(articles)
    var article = articles["Article_" + articleNumber];
    console.log(article)
    setTimeout(function() { createVisualization(article, articleNumber); }, 500);
    });
}

function buildHierarchy(data) {
  var articleID = "Article ID";
  var credCat = "Credibilty Indicator Category";
  var credName = "Credibility Indicator Name";
  var pts = "Points";

  var jsons = {}; //create object of json files
  var currArticle = "Article_" + data[0][articleID];
  var root = {"name": "CATEGORIES", "children": []};
  for (var i = 0; i < data.length; i++) { //article ID column
    if ("Article_" + data[i][articleID] !== currArticle) {
      //adds the current article to jsons, resets root to an empty object
      jsons[currArticle] = root;
      currArticle = "Article_" + data[i][articleID];
      var root = {"name": "CATEGORIES", "children": []};
    }
    if (!checkIn(root["children"], data[i][credCat], "name")) { //credibility column
      //adds a new category to the list of CATEGORIES if it doesn't exist
      root["children"].push({"name": data[i][credCat], "children": []});
    }
    var categoryIndex = findIndex(root["children"], data[i], credCat);

    // .map(function(e) {return e.name}).indexOf(data[i]["Credibilty Indicator Category"]);
    var cin = root["children"][categoryIndex]["children"];
    if (!checkIn(cin, data[i][credName], "name")) { //initializes credibility indicator name and score
      //only adds category if it exists
      cin.push({"name": data[i][credName], "size": 0, "startIndices": [], "endIndices": [], "points": []});
    }
    var cinIndex = findIndex(cin,data[i],  credName);

    // cin.map(function(e) {return e.name}).indexOf(data[i]["Credibility Indicator Name"]);
    cin[cinIndex]["size"] += parseFloat(data[i][pts]); //adds together the net impact of points, has not handled cancellation case, should be calculated based upon absolute value?
    cin[cinIndex]["startIndices"].push(parseInt(data[i]["Start"]));
    cin[cinIndex]["endIndices"].push(parseInt(data[i]["End"]));
    cin[cinIndex]["points"].push(parseInt(data[i][pts]));

  }
  return jsons; //object of json files for each article
};

function checkIn(data, cat, catName) {
  //checks to see if a category exists already
  for (var i = 0; i < data.length; i++) {
    if (data[i][catName] === cat) {
      return true;
    }
  }
  return false;
}

function findIndex(data, currRow, val) {
  /* data takes in the specified information up until the indexing action
  so for example:  if you wanted the Reasoning index, you would put
  root["children"] for data
  currRow is the current row you are on in the CSV.  so for the loop above,
  input data[i], or for row 10, input data[10]
  val is the criteria you want, so "Reasoning", or "Credibilty Indicator Category"
  */
  return data.map(function(e) {return e.name}).indexOf(currRow[val]);
}

function userFindIndex(data, val) {
  /* data:  specified information up until the indexing action
  ex: root["children"]
  val:  criteria desired, ex "Reasoning"
  example:
  userFindIndex(articles["Article_1"]["children"], "Reasoning");
  */
  return data.map(function(e) {return e.name}).indexOf(val);
}



//BEGIN VISUALIZATION
//This is the visualization code.
function createVisualization(article, articleNumber) {
  // Variables
    var width = 300;
    var height = 300;
    var radius = Math.min(width, height) / 2;
    var color = d3.scaleOrdinal(d3.schemeCategory10);

    // Create the "graph" element.
    var g = d3.select('svg')
        .attr("preserveAspectRatio", "xMinYMin meet")
        // .attr("viewBox", "0 0 250 250")
        // .classed("svg-content", true)
        .attr('width', width)
        .attr('height', height)
        .append('g')
        .attr('transform', 'translate(' + width / 2 + ',' + height / 2 + ')');

    // Partitition
    var partition = d3.partition()
        .size([2 * Math.PI, radius]);

    // Find data root
    var root = d3.hierarchy(article)
        .sum(function (d) { return Math.abs(d.size)});

    // Size arcs
    partition(root);
    var arc = d3.arc()
        .startAngle(function (d) { return d.x0 })
        .endAngle(function (d) { return d.x1 })
        .innerRadius(function (d) { return d.y0 })
        .outerRadius(function (d) { return d.y1 });

    // Put it all together


//CATEGORIES SECTION
//This function creates the entirety of the collapsible list.
function createCategories(d, articleNumber) {
    for (var i = 0; i < d.data.children.length; i += 1) {
        var curHTML = document.getElementById("categories").innerHTML;
        var catData = d.data.children[i];
        var catId = catData.name.replace(/ /g,'');
        curHTML = curHTML + "<div id='" + articleNumber + "category" + i + "' class='collapsible'>" + catData.name + "</div>" + "<div id='"  + articleNumber + catId + "' class='content'></div>";
        document.getElementById("categories").innerHTML = curHTML;
        document.getElementById(articleNumber + "category" + i).style.borderLeft = "4px solid " + colorFinder(nameToData.get(catId));
        for (var j = 0; j < catData.children.length; j += 1) {
            var catHTML = document.getElementById(articleNumber + catId).innerHTML;
            var subcatData = catData.children[j];
            var subcatId = catData.children[j].name.replace(/ /g, '');
            //<input type='checkbox' id='c" + j + "' class='check'/>
            if (subcatData.startIndices[j] != -1 && subcatData.endIndices[j] != -1) {
            catHTML = catHTML + "<div class='collapsible'>" + subcatData.name + " </div>" + "<div id='" + articleNumber + subcatId + "' class='content'></div>";
            document.getElementById(articleNumber + catId).innerHTML = catHTML;
            document.getElementById(articleNumber + subcatId).style.backgroundColor = colorFinder(nameToData.get(subcatData.name));
            document.getElementById(articleNumber + subcatId).style.borderLeft = "2px double " + colorFinder(nameToData.get(subcatData.name));
            for (var k = 0; k < subcatData.startIndices.length; k += 1) {
                var subcatHTML = document.getElementById(articleNumber + subcatId).innerHTML;
                var paragraph = document.getElementById("textArticle").innerHTML;

                //Determine the 'clean' start and end of the text.
                var sind = subcatData.startIndices[k];
                var start = (sind + 0);
                var eind = subcatData.endIndices[k];
                var end = (eind + 0);
                end += 1;
                if (dindexToString.get(start) != null) {
                    var existing = dindexToString.get(start)
                    if (existing[0] == 'm') {
                        existing.push(['o', d.data.name]);}
                    else {
                        existing = ['m', existing];
                        existing.push(['o', d.data.name]);
                    }
                    dindexToString.set(start, existing);
                } else {
                    dindexToString.set(start, ["o", d.data.name]);
                }
                if (dindexToString.get(end) != null) {
                    existing = dindexToString.get(end)
                    if (existing[0] == 'm') {
                        existing.push(['c', d.data.name]);
                    } else {
                        existing = ['m', existing];
                        existing.push(['c', d.data.name]);
                    }
                    dindexToString.set(end, existing);
                } else {
                    dindexToString.set(end, ["c", d.data.name]);
                }
                var elipsesstring = "";
                if (end > start + 50) {
                    end = start + 50;
                    elipsesstring = "...";
                }
                var errorString = "'" + paragraph.substring(start, end) + elipsesstring + "'";
                //Need to add in an href for the sake of jumping.
                subcatHTML = subcatHTML + "<a class='jumpable' href='#" + start + "'>" + "[" + subcatData.points[k] + "]  " + errorString + "</a>";
                document.getElementById(articleNumber + subcatId).innerHTML = subcatHTML;
                }
            }
        }
    }
}


//COLORING SECTION
//This function determines the color of a category based on its parent, or name.
  function colorFinder(d) {
    if (d.data.children) {
        if (d.data.name === "Reasoning") {
               return d3.rgb(239, 92, 84);
            } else if (d.data.name === "Evidence") {
               return d3.rgb(0, 165, 150);
            } else if (d.data.name === "Probability") {
                return d3.rgb(0, 191, 255);
            } else if (d.data.name == "Language") {
               return d3.rgb(43, 82, 230);
            }
        }   else {
        //The children node colors are based on the colors of their parents.
            if (d.data.size > 0) {
                return d3.rgb(172,172,172);
            }
            if (d.parent.data.name === "Reasoning") {
                return d3.rgb(237, 134, 88);
            } else if (d.parent.data.name === "Evidence") {
                return d3.rgb(53, 201, 136);
            } else if (d.parent.data.name === "Probability") {
                return d3.rgb(153,204,255);
            } else if (d.parent.data.name == "Language") {
                return d3.rgb(65, 105, 225);
            }
        }
  }

//This function determines the color of text highlights.
  function textHighlight(d) {
      if (d.parent.data.name === "Reasoning") {
          return "red";
      } else if (d.parent.data.name === "Evidence") {
          return "green";
      } else if (d.parent.data.name === "Probability") {
          return "aqua";
      } else {
        return "blue";
      }
  }


//CREDIBILITY VALUES
//These variables ensure that the total value is accurate.
  var sum = 0;
  var total = checkTotal(article);

//This function returns the total number of points in the Credibility Report.
  function checkTotal(d) {
    var top = 0;
    for (i = 0; i < d.children.length; i += 1) {
        for (j = 0; j < d.children[i].children.length; j += 1) {
            top += parseInt(d.children[i].children[j].size);
        }
    }
    return top;
  };

//This function calculates the sum of the child nodes of a category. Used for "Reasoning", "Evidence", "Language".
  function checkSum(d) {
    if (d.data.children) {
        for (i = 0; i < d.data.children.length; i += 1) {
            sum += parseInt(d.data.children[i].size);
        }
    } else {
        sum = parseInt(d.data.size);
    }};


//DICTIONARIES
//This dictionary maps data nodes to paths.
var dataToPath = new Map();
var dataToParentPath = new Map();
var PathToData = new Map();
var nameToData = new Map();

//This dictionary maps indices to strings.
var indexToString = new Map();
var dindexToString = new Map();
var startToPair = new Map();


//These arrays map categories to the names of their children.
var reasoning = [];
var evidence = [];
var language = [];


//CREATE PATHS AND HIGHLIGHTS
//This is the visualization creation in its entirety.\
var counter = 0;
  g.selectAll('g')
            .data(root.descendants())
            .enter()
            .append('g')
            .attr("class", "node")
            .append('path')
            .attr("display", function (d) { return d.depth ? null : "none"; })
            .attr("d", arc)
            //For each path, map it to the corresponding data.
            .each(function(d){
                counter += 1;
                var curPath = this;
                dataToParentPath.set(d, curPath)
                dataToPath.set(d.data, curPath);
                PathToData.set(curPath, d);
                nameToData.set(d.data.name, d)
                if (d.data.children) {
                    for (var i = 0; i < d.data.children.length; i += 1) {
                        if (d.data.name == "Reasoning") {
                            reasoning[i] = d.data.children[i].name;
                        } else if (d.data.name == "Evidence") {
                            evidence[i] = d.data.children[i].name;
                        } else {
                            language[i] = d.data.children[i].name;
                        }
                    }
                    return;
                } else {
                    var paragraph = document.getElementById("textArticle").innerHTML;
                    for (i = 0; i < d.data.startIndices.length; i += 1) {
                        if (d.data.startIndices[i] != -1 && d.data.endIndices[i] != -1) {
                            var ind = d.data.startIndices[i];
                            var start = (ind + 0);
                            var ind = d.data.endIndices[i];
                            var end = (ind + 0);
                            end += 1;

                            if (indexToString.get(start) != null) {
                                var existing = indexToString.get(start)
                                if (existing[0] == 'm') {
                                    existing.push(['o', textHighlight(d), d.data.name]);
                                } else {
                                    existing = ['m', existing];
                                    existing.push(['o', textHighlight(d), d.data.name]);
                                }
                                indexToString.set(start, existing);
                            } else {
                                indexToString.set(start, ["o", textHighlight(d), d.data.name]);
                            }
                            if (indexToString.get(end) != null) {
                                existing = indexToString.get(end)
                                if (existing[0] == 'm') {
                                    existing.push(['c', textHighlight(d), d.data.name]);
                                } else {
                                    existing = ['m', existing];
                                    existing.push(['c', textHighlight(d), d.data.name]);
                                }
                                indexToString.set(end, existing);
                            } else {
                                indexToString.set(end, ["c", textHighlight(d), d.data.name]);
                            }
                            startToPair.set(start, end);
                        }
                    }
                    //This completes the creation of the dictionary, with each entry having a unique key.
                }
            })

createCategories(nameToData.get("CATEGORIES"), articleNumber);

//This code adds in the highlights as needed.
var unsorted = Array.from(indexToString.keys());
var sorted = unsorted.sort(function(a, b){return a - b});
var indexOffset = 0;
var numactive = 0;
var currentHighlight = "";
var inputString = "";
var categoryColor = "";
var ancient = "transparent";
var ancName = "";
var hancName = "";
var oldest = "transparent";
var oldName = "";
var holdName = "";
var middle = "transparent";
var midName = "";
var hmidName = "";
var newest = "transparent";
var newName = "";
var hnewName = "";
console.log(sorted)
for (i = 0; i < sorted.length; i += 1) {
    currentHighlight = indexToString.get(sorted[i]);
    //console.log("Index: " + sorted[i] + " and array: " + currentHighlight)
    if (currentHighlight[0] == "m") {
        var opens = [];
        var closes = [];
        for (j = 1; j < currentHighlight.length; j+= 1) {
            if (currentHighlight[j][0] == "c") {
                closes.push(j);
            } else if (currentHighlight[j][0] == "o") {
                opens.push(j);
            }
        }
        console.log(currentHighlight)
        if (opens.length == 2 && numactive == 0) {
            var nextHighlight = currentHighlight[2];
            currentHighlight = currentHighlight[1];

            newest = currentHighlight[1];
            hnewName = currentHighlight[2];
            newName = currentHighlight[2].replace(/ /g,'');

            currentHighlight = nextHighlight;
            middle = newest;
            hmidName = hnewName;
            midName = newName;
            newest = currentHighlight[1];
            hnewName = currentHighlight[2];
            newName = currentHighlight[2].replace(/ /g,'');
            inputString = "<" + midName + newName + " id='" + sorted[i] + "' name='" + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                middle + " 0%, transparent 20%, transparent 35%, " +
                newest + " 40%, transparent 55%, transparent 70%)" +
                "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
            numactive = 2;
        } else if (opens.length == 3 && numactive == 0) {
            var cur1 = currentHighlight[1];
            var cur2 = currentHighlight[2];
            var cur3 = currentHighlight[3];
            newest = cur1[1];
            hnewName = cur1[2];
            newName = cur1[2].replace(/ /g,'');
            middle = cur2[1];
            hmidName = cur2[2];
            midName = cur2[2].replace(/ /g,'');
            oldest = cur3[1];
            holdName = cur3[2];
            oldName = cur3[2].replace(/ /g,'');
            inputString = "<" + oldName + midName + newName + " id='" + sorted[i] + "' name='" + holdName + ", " + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                oldest + " 0%, transparent 20%, transparent 35%, " +
                middle + " 40%, transparent 55%, transparent 70%, " +
                newest + " 75%, transparent 90%)" +
                "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
            numactive = 3;
        } else if (opens.length == 2 && numactive == 1 && closes.length == 0) {

        } else if (closes.length == 2 && numactive == 2) {
            inputString = "<hiText class='highlightertext'>" + hmidName + ", " + hnewName + "</hiText></" + midName + newName +">";
            numactive = 0;
            middle = oldest;
            hmidName = holdName;
            midName = oldName;
            newest = middle;
            hnewName = hmidName;
            newName = midName;
        } else if (closes.length == 3 && numactive == 3) {
            inputString = "<hiText class='highlightertext'>" + holdName + ", " + hmidName + ", " + hnewName + "</hiText></" + oldName + midName + newName +">";
            numactive = 0;
            oldest = ancient;
            holdName = hancName;
            oldName = ancName;
            middle = oldest;
            hmidName = holdName;
            midName = oldName;
            newest = middle;
            hnewName = hmidName;
            newName = midName;
        }
    } else if (currentHighlight[0] == "o") {
        if (numactive == 4) {                                                                           //Catcher: This catches the more than four case.
            indexToString.get(startToPair.get(sorted[i]))[0] = "f";
            continue;
        }
        ancient = oldest;
        hancName = holdName;
        ancName = oldName;
        oldest = middle;
        holdName = hmidName;
        oldName = midName;
        middle = newest;
        hmidName = hnewName;
        midName = newName;
        newest = currentHighlight[1];
        hnewName = currentHighlight[2];
        newName = currentHighlight[2].replace(/ /g,'');
        var endString = "";

        //Need to add in an id for the sake of jumping.
        if (numactive == 0) {
            inputString = "<" + newName + " id='" + sorted[i] + "' name='" + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                        newest + " 0%, transparent 20%)" +
                        "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
            numactive = 1;
        } else if (numactive == 1) {
            inputString = "<" + midName + newName + " id='" + sorted[i] + "' name='" + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                        middle + " 0%, transparent 20%, transparent 35%, " +
                        newest + " 40%, transparent 55%, transparent 70%)" +
                        "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
            endString = "<hiText class='highlightertext'>" + hmidName + "</hiText></" + midName +">";
            numactive = 2;
        } else if (numactive == 2) {
            inputString = "<" + oldName + midName + newName + " id='" + sorted[i] + "' name='" + holdName + ", " + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                        oldest + " 0%, transparent 20%, transparent 35%, " +
                        middle + " 40%, transparent 55%, transparent 70%, " +
                        newest + " 75%, transparent 90%)" +
                        "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
            endString = "<hiText class='highlightertext'>" + holdName + ", " + hmidName + "</hiText></" + oldName + midName + ">";
            numactive = 3;
        } else if (numactive == 3) {
            inputString = "<" + ancName + oldName + midName + newName + " id='" + sorted[i] + "' name='" + hancName + ", " + holdName + ", " + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                        ancient + " 0%, transparent 15%, transparent 25%, " +
                        oldest + " 30%, transparent 45%, transparent 55%, " +
                        middle + " 60%, transparent 75%, transparent 85%, " +
                        newest + " 90%, transparent 99%)" +
                        "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
            endString = "<hiText class='highlightertext'>" + hancName + ", " + holdName + ", " + hmidName + "</hiText></" + ancName + oldName + midName + ">";
            numactive = 4;
        }
        inputString = endString + inputString;
    } else if (currentHighlight[0] == "c") {
        categoryName = currentHighlight[2].replace(/ /g,'');
        var continueString = "";
        var closedSomething = 0;
        if (numactive == 1) {
            if (newName == categoryName) {
                closedSomething = 1;
            }
        } else if (numactive == 2) {
            if (newName == categoryName) {
                continueString = "<" + midName + " id='" + sorted[i] + "' name='" + hmidName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    middle + " 0%, transparent 20%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            } else if (midName == categoryName) {
                continueString = "<" + newName + " id='" + sorted[i] + "' name='" + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    newest + " 0%, transparent 20%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            }
        } else if (numactive == 3) {
            if (newName == categoryName) {
                continueString = "<" + oldName + midName + " id='" + sorted[i] + "' name='" + holdName + ", " + hmidName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    oldest + " 0%, transparent 20%, transparent 35%, " +
                                    middle + " 40%, transparent 55%, transparent 70%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            } else if (midName == categoryName) {
                continueString = "<" + oldName + newName + " id='" + sorted[i] + "' name='" + holdName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    oldest + " 0%, transparent 20%, transparent 35%, " +
                                    newest + " 40%, transparent 55%, transparent 70%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            } else if (oldName == categoryName) {
                continueString = "<" + midName + newName + " id='" + sorted[i] + "' name='" + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    middle + " 0%, transparent 20%, transparent 35%, " +
                                    newest + " 40%, transparent 55%, transparent 70%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            }
        } else {
            if (newName == categoryName) {
                continueString = "<" + ancName + oldName + midName + " id='" + sorted[i] + "' name='" + hancName + ", " + holdName + ", " + hmidName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    ancient + " 0%, transparent 20%, transparent 35%, " +
                                    oldest + " 40%, transparent 55%, transparent 70%, " +
                                    middle + " 75%, transparent 90%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            } else if (midName == categoryName) {
                continueString = "<" + ancName + oldName + newName + " id='" + sorted[i] + "' name='" + hancName + ", " + holdName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    ancient + " 0%, transparent 20%, transparent 35%, " +
                                    oldest + " 40%, transparent 55%, transparent 70%, " +
                                    newest + " 75%, transparent 90%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            } else if (oldName == categoryName) {
                continueString = "<" + ancName + midName + newName + " id='" + sorted[i] + "' name='" + hancName + ", " + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    ancient + " 0%, transparent 20%, transparent 35%, " +
                                    middle + " 40%, transparent 55%, transparent 70%, " +
                                    newest + " 75%, transparent 90%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            } else if (ancName == categoryName) {
                continueString = "<" + oldName + midName + newName + " id='" + sorted[i] + "' name='" + holdName + ", " + hmidName + ", " + hnewName + "' class='highlighter' style='background: linear-gradient(to bottom, " +
                                    oldest + " 0%, transparent 20%, transparent 35%, " +
                                    middle + " 40%, transparent 55%, transparent 70%, " +
                                    newest + " 75%, transparent 90%)" +
                                    "; background-position: 0 1.1em; background-repeat: repeat-x; background-size: 2px 13px; padding-bottom: 15px'>";
                closedSomething = 1;
            }
        }
        //if (closedSomething == 0) {continue;}
        if (numactive == 1) {
            inputString = "<hiText class='highlightertext'>" + hnewName + "</hiText></" + newName +">" + continueString;
        } else if (numactive == 2) {
            inputString = "<hiText class='highlightertext'>" + hmidName + ", " + hnewName + "</hiText></" + midName + newName +">" + continueString;
        } else if (numactive == 3) {
            inputString = "<hiText class='highlightertext'>" + holdName + ", " + hmidName + ", " + hnewName + "</hiText></" + oldName + midName + newName +">" + continueString;
        } else {
            inputString = "<hiText class='highlightertext'>" + hancName + ", " + holdName + ", " + hmidName + ", " + hnewName + "</hiText></" + ancName + oldName + midName + newName +">" + continueString;
        }
        if (newName == categoryName) {
            newest = middle;
            hnewName = hmidName;
            newName = midName;
            middle = oldest;
            hmidName = holdName;
            midName = oldName;
            oldest = ancient;
            holdName = hancName;
            oldName = ancName;
            ancient = "transparent";
            hancName = "";
            ancName = "";
        } else if (midName == categoryName) {
            middle = oldest;
            hmidName = holdName;
            midName = oldName;
            oldest = ancient;
            holdName = hancName;
            oldName = ancName;
            ancient = "transparent";
            hancName = "";
            ancName = "";
        } else if (oldName == categoryName) {
            oldest = ancient;
            holdName = hancName;
            oldName = ancName;
            ancient = "transparent";
            hancName = "";
            ancName = "";
        } else {
            ancient = "transparent";
            hancName = "";
            ancName = "";
        }
        numactive -= 1;
    }
    var paragraph = document.getElementById("textArticle").innerHTML;
    paragraph = paragraph.substring(0, sorted[i] + indexOffset) + inputString + paragraph.substring(sorted[i] + indexOffset);
    document.getElementById("textArticle").innerHTML = paragraph;
    indexOffset += inputString.length;
}

//VISUALIZATION TEXTBOX
//This section enables the textbox on hover in the article itself.
var hText = document.querySelectorAll('.highlightertext');

var visibleArc = false;
var $cols = $('.highlighter').hover(function(e) {
    var currCategory = $(this).attr("name");
    var d = nameToData.get(currCategory);
    var numCategories = 0;
    var dArray = [];

    d3.selectAll("path")
        .transition()
        .style("opacity", 0.5)
        .duration(100)

    for (var j = 0; j < Array.from(nameToData.keys()).length; j += 1) {
        if (currCategory.includes(Array.from(nameToData.keys())[j])) {
            dArray.push(nameToData.get(Array.from(nameToData.keys())[j]));
            numCategories += 1;
        }
    }
        //allows for arcs to disappear
    if (visibleArc == false) {
        visibleArc = true;
        for (var k = 0; k < numCategories; k += 1) {
                d = dArray[k];
                var highlightParent = d.parent.data;
                var highlightPath = dataToPath.get(d);

            //activates half opaque arcs
                for (var i = 0; i < highlightParent.children.length; i += 1) {
                    d3.select(dataToPath.get(highlightParent.children[i]))
                        .transition()
                        .style("display", "block")
                        .style("opacity", 0.5)
                        .duration(100)
                }
                d3.select(dataToParentPath.get(d.parent))
                    .transition()
                    .duration(300)
                    .attr('stroke-width',5)
                    .style("opacity", 1)

                d3.select(dataToParentPath.get(d))
                    .transition()
                    .attr('stroke-width',5)
                    .style("opacity", 1)

                g.selectAll(".center-text")
                    .style("display", "none")
                d3.select(dataToParentPath.get(d))
                    .transition()
                    .attr('stroke-width',5)
                    .style("opacity", 1)
                checkSum(d);
                g.append("text")
                    .attr("class", "center-text")
                    .attr("x", -5)
                    .attr("y", 5)
                    .style("font-size", 40)
                    .style("text-anchor", "middle")
                    .html(sum)
                                                        //PROTOTYPE CODE
                psuedobox.transition()
                    .duration(200)
                    .style("opacity", .9);
                psuedobox.html(currCategory)
                    .style("left", 80 + "%")
                    .style("top", 30 + "%")
                    .style("width", function() {
                        if (currCategory.length < 18) {
                            return "90px";
                        } else if (currCategory.length < 35) {
                            return "180px";
                        } else {
                            return "250px";
                        }
                    })
                    .style("height", function() {
                        if (currCategory.length > 50) {
                            return "60px";
                        }
                    })
            this.style.backgroundColor = colorFinder(d);
        }
    } else {
    //takes out all present visuals
        visibleArc = false;
        resetVis();
        psuedobox.transition()
           .duration(200)
           .style("opacity", 0);
        this.style.backgroundColor = "white";
    }
});

window.onmousemove = function (e) {
    var x = (e.clientX + 25) + 'px',
        y = (e.clientY + - 18) + 'px';
    for (var i = 0; i < hText.length; i++) {
        hText[i].style.top = y;
        hText[i].style.left = x;
    }
};
//This section enables the Floating Textbox for the visualization.
var visBox = d3.select("chart").append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

var psuedobox = d3.select("chart").append("div")
    .attr("class", "psuedobox")
    .style("opacity", 0);


//VISUALIZATION ANIMATIONS
//This removes the current effects and resets the visualization to default.
var visOn = false;
function resetVis() {
    d3.selectAll("path")
        .transition()
        .delay(300)
        .duration(800)
        .attr('stroke-width',2)
        .style("opacity", function(d) {
            if (d.data.children) {
            } else {
                return 0;
            }
        })
    d3.selectAll("path")
        .transition()
        .delay(1000)
        .attr('stroke-width',2)
        .style("display", function(d) {
            if (d.data.children) {
            } else {
                return "none";
            }
        })
    g.selectAll(".center-text")
        .style("display", "none")
    sum = 0;
    g.append("text")
        .attr("class", "center-text")
        .attr("x", 0)
        .attr("y", 13)
        .style("font-size", 50)
        .style("text-anchor", "middle")
        .html((100 + total))
    visBox.transition()
            .duration(200)
            .style("opacity", 0);
    visOn = false;
    }

resetVis();

d3.selectAll("path").transition().each(function(d) {
    if (!d.data.children) {
        this.style.display = "none";
    } else if (d.data.name == "CATEGORIES") {
        this.style.display = "none";
    }
})

//MOUSE ANIMATION
  //This is all the mouse animation code.
  g.selectAll('path')
            //On mouse entering, highlight path, clear old text, create new text.
            .on('mouseover',function(d) {
                var curPath = this;
                g.selectAll(".center-text")
                    .style("display", "none")

                d3.selectAll("path").transition().style("opacity", 0.5);
                if (d.data.children) {
                    for (var i = 0; i < d.data.children.length; i += 1) {
                        d3.select(dataToPath.get(d.data.children[i]))
                            .transition()
                            .style("display", "block")
                            .style("opacity", 0.5)
                            .duration(100)
                    }
                } else {
                    var dataArray = Array.from(dataToParentPath.keys());
                    for (var i = 0; i < Array.from(dataToParentPath.keys()).length; i += 1) {
                        if (PathToData.get(curPath).parent == dataArray[i].parent) {
                            dataToPath.get(dataArray[i].data).style.opacity = 0.5;
                        }
                    }
                }

                d3.select(this)
      	            .transition()
      	            .duration(300)
      	            .attr('stroke-width',5)
      	            .style("opacity", 1)
      	        d3.select(dataToParentPath.get(d.parent))
      	            .transition()
      	            .duration(300)
      	            .attr('stroke-width',5)
      	            .style("opacity", 1)
      	        if (d.height == 0) {
      	            var allelems = document.querySelectorAll("[name]");
                    for (var i = 0; i < allelems.length; i += 1) {
                        if (allelems[i].attributes.name.nodeValue.includes(d.data.name)) {
                            allelems[i].style.backgroundColor = colorFinder(d);
                        }
                    }
      	        }

      	        //This code creates the text in the center of the model.
      	        checkSum(d)
                g.append("text")
                    .attr("class", "center-text")
                    .attr("x", -5)
                    .attr("y", 5)
                    .style("font-size", 40)
                    .style("text-anchor", "middle")
                    .html(sum)
                visBox.transition()
                    .duration(200)
                    .style("opacity", .9);
                visBox.html(d.data.name)
                    .style("left", (d3.event.pageX) + "px")
                    .style("top", (d3.event.pageY - 28) + "px")
                    .style("width", function() {
                        if (d.data.name.length < 18) {
                            return "90px";
                        } else {
                            return "180px";
                        }
                    })
                visOn = true;
            })
            .on("mousemove", function(){
                if (visOn == true) {
                    visBox
                        .style("left", (d3.event.pageX)+ "px")
                        .style("top", (d3.event.pageY - 28) + "px")
                } else {
                    visBox.transition()
                        .duration(10)
                        .style("opacity", 0);
                }})
            //On mouse exiting, remove all highlights and clear all text and display the total value.
            .on('mouseout',function (d) {
                var allelems = document.querySelectorAll("[name]");
                for (var i = 0; i < allelems.length; i += 1) {
                    if (allelems[i].nodeName.includes(d.data.name.replace(/ /g,'').toUpperCase())) {
                        allelems[i].style.backgroundColor = "white";
                    }
                }
                resetVis();
            })
            .on('mousedown', function (d) {
            //autoscroll to section functionality
                if (d.height == 0) {
                    $('html,body').animate({
                        scrollTop: $("#" + d.data.startIndices[0]).offset().top -500},'slow'); //******
                }
            })
            .style('stroke', 'white')
            .attr('stroke-width', 2)
            .style("fill", colorFinder);

var coll = document.getElementsByClassName('collapsible');
var i;
for (i = 0; i < coll.length; i++) {
    coll[i].addEventListener("click", function() {
        /*var checkElem = document.getElementById(this.innerHTML.substring(27, 29));
        if (checkElem) {
            if (checkElem.checked) {
                var allelems = document.querySelectorAll("[name]");
                for (var i = 0; i < allelems.length; i += 1) {
                    if (allelems[i].attributes.name.nodeValue.includes(this.id)) {
                        allelems[i].style.backgroundColor = d3.rgb(237, 134, 88);
                    }
                }
            } else {
                var allelems = document.querySelectorAll("[name]");
                for (var i = 0; i < allelems.length; i += 1) {
                    if (allelems[i].attributes.name.nodeValue.includes(this.id)) {
                        allelems[i].style.backgroundColor = d3.rgb(255,255,255);
                    }
                }
            }
        }*/
        this.classList.toggle("active");
        var content = this.nextElementSibling;
        if (content.style.display === "block") {
            content.style.display = "none";
        } else {
            content.style.display = "block";
        }
    });
   }

$(document).ready(function(){
    $("a").on('click', function(event) {
        if (this.hash !== "") {
            event.preventDefault();
            var hash = this.hash;

            $(hash).animate({backgroundColor: "yellow"}, 500).animate({backgroundColor: "white"}, 500).animate({backgroundColor: "yellow"}, 500).animate({backgroundColor: "white"}, 500);

            $('html, body').animate({
                //scrollTop: $(hash).offset().top - 500}, 800, function(){window.location.hash = hash;}
                scrollTop: $(hash).offset().top -500},'slow');
        }
    });
});
}
