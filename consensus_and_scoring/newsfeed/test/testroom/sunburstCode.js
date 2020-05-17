//Use this to control which csv and txt are being used.
function runVisualization(articleNumber) {
    //This section parses the CSV file into a JSON.
    d3.csv("VisualizationData_" + articleNumber + ".csv", function(error, data) {
    if (error) throw error;
    var articles = buildHierarchy(data);
    var article1 = articles["Article_" + articleNumber];
    console.log(articles)
    createVisualization(article1);
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
function createVisualization(article) {
  // Variables
    var chartDiv = document.getElementById("chart"); //resizes with css

    var width = 600;
    var height = 600;
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
        } else {
            return d3.rgb(43, 82, 230);
        }
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
var dataToParentPath = new Map();

//CREATE PATHS AND HIGHLIGHTS
//This is the visualization creation in its entirety.\
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
                var curPath = this;
                dataToParentPath.set(d, curPath)
             })

//VISUALIZATION ANIMATIONS
//This removes the current effects and resets the visualization to default.
var visOn = false;
function resetVis() {
    d3.selectAll("path")
        .transition()
        .delay(300)
        .duration(800)
        .attr('stroke-width',7)
        .style("opacity", function(d) {
            if (d.data.children) {
            } else {
                return 0;
            }
        })
    d3.selectAll("path")
        .transition()
        .delay(1000)
        .attr('stroke-width',7)
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
        .attr("x", -5)
        .attr("y", 30)
        .style("font-size", 100)
        .style("font-family", "'Roboto', sans-serif")
        .style("text-anchor", "middle")
        .html((100 + total))
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
                d3.select(this)
      	            .transition()
      	            .duration(300)
      	            .attr('stroke-width',7)
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
                    .attr("y", 30)
                    .style("font-size", 100)
                    .style("font-family", "'Roboto', sans-serif")
                    .style("text-anchor", "middle")
                    .html(sum)
                visOn = true;
            })
            //On mouse exiting, remove all highlights and clear all text and display the total value.
            .on('mouseout',function (d) {
                resetVis();
            })
            .style('stroke', 'white')
            .attr('stroke-width', 7)
            .style("fill", colorFinder);
}
