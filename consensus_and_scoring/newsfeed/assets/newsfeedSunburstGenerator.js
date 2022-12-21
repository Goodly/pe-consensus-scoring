/** This file will create the hallmark. It uses the d3 library to create a sunburst visualization.
A rough roadmap of the contents:
    - global variables
    - create hallmark skeleton
    - fill center of hallmark
    - mouse animations
    - helper functions

**/

var width = 200,
    height = 200,
    radius = (Math.min(width, height) / 2) + 10;

var formatNumber = d3.format(",d");

var x = d3.scaleLinear()
    .range([0, 2 * Math.PI]);

var y = d3.scaleSqrt()
    .range([0, radius]);

var color = d3.scaleOrdinal(d3.schemeCategory10);

var partition = d3.partition();
var NUM_NFC = {};



/* A map that relates a node in the data heirarchy to the
SVGPathElement in the visualization.
*/
var nodeToPath = new Map();

var arc = d3.arc()
    .startAngle(function(d) { return Math.max(0, Math.min(2 * Math.PI, x(d.x0))); })
    .endAngle(function(d) { return Math.max(0, Math.min(2 * Math.PI, x(d.x1))); })
    .innerRadius(function(d) { return 130 *d.y0; })
    .outerRadius(function(d) { return 130 * d.y1; });


//This variable creates the floating textbox on the hallmark
var DIV;

async function hallmark(entry) {
  const dataFileName = entry.highlightData;
  const triageDataFileName = entry.triageData;
  const id = entry.sha256;
  var svg = d3.select("body").append("svg")
    .attr("articleID", id)
    .attr("width", width)
    .attr("height", height)
    .append('g')
    .attr("transform", "translate(" + (width / 2) + "," + (height / 2) + ")");

  var visualizationOn = false;

  var div = d3.select("body").append("div")
      .attr("class", "tooltip")
      .style("opacity", 0);


  //This code block takes the csv and creates the visualization.
  return d3.csv(dataFileName, function(error, data) {
    if (error) {
      console.log(error);
      return;
    }
    d3.csv(triageDataFileName, function(error, triageData) {
      if (error) {
        console.err(error);
        return;
      }
      moveFactCheckLabels(triageData, data, entry.sha256);
      delete data["columns"];
      clean(data);
      data = addDummyData(data);
      var root = convertToHierarchy(data);
      condense(root);
      var holistic_score = 0;
      for (let [key, value] of HOLISTIC_MAP) {
        holistic_score += Math.round(value);
      }
      totalScore = 90 + scoreSum(root) + holistic_score;
      entry.credibilityScore = totalScore;
      root.sum(function(d) {
        return Math.abs(parseFloat(d.data.Points));
      });

      //Fill in the colors
      console.log('Creating vis for ' + entry.sha256)
      svg.selectAll("path")
        .data(partition(root).descendants())
        .enter().append("path")
          .attr("d", arc)
          .style("fill", function(d) {
            nodeToPath.set(d, this);
            return color(d.data.data["Credibility Indicator Category"]);
          }).style("display", function(d) {
            if (d.height == 0 || d.height == 2) {
                return "none";
            }
        });
      var center_style = getCenterStyle(NUM_NFC[entry.sha256]);
      var text_x = center_style[0];
      var text_y = center_style[1];
      var text_size = center_style[2];
      var question_x = center_style[3];
      var question_y = center_style[4];
      var question_size = center_style[5]
      var double_question = center_style[6];

      //Setting the center circle to the score
      svg.selectAll(".center-text")
        .style("display", "none")
      svg.append("text")
        .attr("class", "center-text")
        .attr("x", text_x)
        .attr("y", text_y)
        .style("font-size", text_size)
        .style("text-anchor", "middle")
        .html((totalScore));

      if (double_question) {
        svg.append("text")
          .attr("class", "center-question")
          .attr("x", question_x)
          .attr("y", question_y)
          .style("font-size", question_size)
          .html("??")
      } else {
        svg.append("text")
          .attr("class", "center-question")
          .attr("x", question_x)
          .attr("y", question_y)
          .style("font-size", question_size)
          .html("?")
      }

      //Setting the outer and inside rings to be transparent.
      d3.selectAll("path").transition().each(function(d) {
          if (!d.children) {
              this.style.display = "none";
          } else if (d.height == 2) {
              this.style.opacity = 0;
          }
      });



    //Mouse animations.
    svg.selectAll('path')
        .on('mouseover', function(d) {
            if (d.height == 2) {
                return;
            }
            console.log(NUM_NFC[entry.sha256]);
            d3.select(nodeToPath.get(d))
          	            .transition()
          	            .duration(300)
          	            .attr('stroke-width',3)
          	            .style("opacity", .8)
            div.transition()
                .duration(200)
                .style("display", "block")
                .style("opacity", .9);
            div.html(d.data.data['Credibility Indicator Name'])
                .style("left", (d3.event.pageX) + "px")
                .style("top", (d3.event.pageY) + "px")
                .style("width", "100px");

        var pointsGained = scoreSum(d);
        svg.selectAll(".center-text").style('display', 'none');
        svg.selectAll(".center-question").style('display', 'none');
        if (d.data.data["Credibility Indicator Name"] == "Evidence") {
          var child;
          var allFactCheck = true;
          for (child of d.children) {

            if (child.data.data["Credibility Indicator Name"] != "Waiting for fact-checkers") {
              allFactCheck = false;
            }
          }
          if (allFactCheck) {
            pointsGained = "?";
          }
        }

        svg.append("text")
            .attr("class", "center-text")
            .attr("x", 0)
            .attr("y", 13)
            .style("font-size", 40)
            .style("text-anchor", "middle")
            .html((pointsGained));
        div
            .style("opacity", .7)
            .style("left", (d3.event.pageX)+ "px")
            .style("top", (d3.event.pageY - 28) + "px");
            visualizationOn = true;

        })
        .on('mousemove', function(d) {
            if (visualizationOn) {
            div
                .style("left", (d3.event.pageX)+ "px")
                .style("top", (d3.event.pageY - 28) + "px")
            } else {
                div.transition()
                    .duration(10)
                    .style("opacity", 0);
                    
            }
        })
        .on('mouseleave', function(d) {
          var pointsGained = totalScore = 90 + scoreSum(root) + holistic_score;
          d3.select(nodeToPath.get(d))
              .transition()
              .duration(300)
              .attr('stroke-width', 2)
              .style("opacity", 1)

          div.transition()
                  .delay(200)
                  .duration(600)
                  .style("opacity", 0);
          
          var center_style = getCenterStyle(NUM_NFC[entry.sha256]);
          var text_x = center_style[0];
          var text_y = center_style[1];
          var text_size = center_style[2];
          var question_x = center_style[3];
          var question_y = center_style[4];
          var question_size = center_style[5]
          var double_question = center_style[6];


          svg.selectAll(".center-text").style('display', 'none');
          svg.selectAll(".center-question").style('display', 'none');
          svg.append("text")
              .attr("class", "center-text")
              .attr("x", text_x)
              .attr("y", text_y)
              .style("font-size", text_size)
              .style("text-anchor", "middle")
              .html((pointsGained));

          if (double_question) {
            svg.append("text")
                .attr("class", "center-question")
                .attr("x", question_x)
                .attr("y", question_y)
                .style("font-size", question_size)
                .html("??")
          } else {
            svg.append("text")
                .attr("class", "center-question")
                .attr("x", question_x)
                .attr("y", question_y)
                .style("font-size", question_size)
                .html("?")
          }
        })
        .style("fill", colorFinderSun);
        visualizationOn = false;
    });
  });

}


/*** HELPER FUNCTIONS ***/

/* Function that provides the color based on the node.
    @param d: the node in the data heirarchy
    @return : a d3.rgb object that defines the color of the arc
*/

function colorFinderSun(d) {
    if (d.height == 2) {
        return d3.rgb(0, 0, 0);
    }
    if (d.data.children) {
        if (d.data.data['Credibility Indicator Name'] == "Reasoning") {
               return d3.rgb(239, 117, 89);
            } else if (d.data.data['Credibility Indicator Name'] == "Evidence") {
               return d3.rgb(87, 193, 174);
            } else if (d.data.data['Credibility Indicator Name'] == "Probability") {
                return d3.rgb(118,188,226);
            } else if (d.data.data['Credibility Indicator Name'] == "Language") {
               return d3.rgb(75, 95, 178);
            } else if (d.data.data['Credibility Indicator Name'] == "Holistic"){
                return d3.rgb(255, 180, 0);
            } else if (d.data.data['Credibility Indicator Name'] == "Sourcing") {
              return d3.rgb(167, 67, 224);
            } else {
              return d3.rgb(255, 255, 255);
            }
        }
  }


/* Function that resets the visualization after the mouse has been moved
   away from the sunburst. It resets the text score to the original
   article score and resets the colors to their original.
   @param d : the node in the data heirarchy
   @return : none
*/
function resetVis(d, graphObject) {
    normalSun(d);
    d3.selectAll("path")
        .transition()
        .delay(300)
        .duration(800)
        .attr('stroke-width',2)
        .style("opacity", function(d) {
            if (d.height == 1) {
            } else {
                return 0;
            }
        })
    d3.selectAll("path")
        .transition()
        .delay(1000)
        .attr('stroke-width',2)
        .style("display", function(d) {
            if (d.children) {
            } else {
                return "none";
            }
        })
    DIV.transition()
            .delay(200)
            .duration(600)
            .style("opacity", 0);
    var total = parseFloat(scoreSum(d));
    graphObject.selectAll(".center-text").style('display', 'none');


    var center_style = getCenterStyle(NUM_NFC[entry.sha256]);
    var text_x = center_style[0];
    var text_y = center_style[1];
    var text_size = center_style[2];
    var question_x = center_style[3];
    var question_y = center_style[4];
    var question_size = center_style[5]
    var double_question = center_style[6];


    graphObject.selectAll(".center-text").style('display', 'none');
    graphObject.selectAll(".center-question").style('display', 'none');
    graphObject.append("text")
        .attr("class", "center-text")
        .attr("x", text_x)
        .attr("y", text_y)
        .style("font-size", text_size)
        .style("text-anchor", "middle")
        .html((totalScore));

    if (double_question) {
      graphObject.append("text")
          .attr("class", "center-question")
          .attr("x", question_x)
          .attr("y", question_y)
          .style("font-size", question_size)
          .html("??")
    } else {
      graphObject.append("text")
          .attr("class", "center-question")
          .attr("x", question_x)
          .attr("y", question_y)
          .style("font-size", question_size)
          .html("?")
    }
}

/*Function that draws the visualization based on what is being hovered over.
    @param d : the node in the data heirarchy that I am hovering over
    @param root : the root of the data heirarchy
    @param me : the path that I am hovering over.
    @return : none
*/
function drawVis(d, root, me, graphObject) {
    if (d.height == 2) {
        resetVis(d, graphObject);
        return;
    }
    d3.selectAll("path")
        .transition()
        .style("opacity", function(d) {
            return .5
            }
        );

    d3.select(me)
        .transition()
        .duration(300)
        .attr('stroke-width', 5)
        .style("opacity", 1)

    if (d.height == 0) {
        d3.select(nodeToPath.get(d.parent))
            .transition()
            .duration(300)
            .attr('stroke-width', 5)
            .style("opacity", 1)
    } if (d.height == 0) {
        let textToHighlight = document.getElementsByName(d.data.data["Credibility Indicator ID"] + "-" + d.data.data.Start + "-" + d.data.data.End);
        highlightSun(textToHighlight[0]);
    }
    else if (d.height == 2) {
        d3.select(me).style('display', 'none');
    } else if (d.height == 1) {
        d3.select(nodeToPath.get(d.parent)).style('display', 'none');
    }

    DIV.transition()
            .duration(200)
            .style("opacity", .9);
        DIV.html(d.data.data['Credibility Indicator Name'])
            .style("left", (d3.event.pageX) + "px")
            .style("top", (d3.event.pageY) + "px")
            .style("width", function() {
                if (d.data.data['Credibility Indicator Name'].length < 10) {
                    return "90px";
                } else {
                    return "180px";
                }
            });

    var pointsGained = scoreSum(d);
    graphObject.selectAll(".center-text").style('display', 'none');
    graphObject.append("text")
        .attr("class", "center-text")
        .attr("x", 0)
        .attr("y", 13)
        .style("font-size", 40)
        .style("text-anchor", "middle")
        .html((pointsGained));
}




/*
Recursive function that returns a number that represents the total score of the given arc.
For the center, we simply return the score of the article (90 plus the collected points).
    @param d = the node of the hierarchy.
    @return : the cumulative score of a certain path.
              These are the points lost. The
              scoreSum(root) of an article with no
              points lost would be 0.
*/
function scoreSum(d) {
    if (d.data.data.Points) {
        if (d.data.data["Credibility Indicator Name"] == "Waiting for fact-checkers") {
          return 0;
        }
        return Math.round(d.data.data.Points);
    } else {
        var sum = 0;
        for (var i = 0; i < d.children.length; i++) {
            sum += Math.round(parseFloat(scoreSum(d.children[i])));
        }
        if (d.height == 2) {
            articleScore = parseFloat(sum);
            return Math.round(articleScore);
        }
        return sum;
    }
}

function moveFactCheckLabels(triageData, visDataArray, id) {
  NUM_NFC[id] = 0;
  var item;
  var maxEvidenceID = -1;

  for (item of visDataArray) {
    if (item["Credibility Indicator Category"] == "Evidence") {
      var id = item["Credibility Indicator ID"];
      var id_num = parseInt(id.substring(1, 2));
      maxEvidenceID = Math.max(id_num, maxEvidenceID);
    }
  }
  var factCheckID = maxEvidenceID + 1;

  var caseNumbersSoFar = [];
  var triageRow;
  for (triageRow of triageData) {
    if (triageRow["topic_name"] == "Needs Fact-Check") { // You'll need to change this
      // data[rowIndex]
      var newVisRow = Object.assign({}, visDataArray[0]);


      if (!(caseNumbersSoFar.includes(triageRow["case_number"]))) {
        caseNumbersSoFar.push(triageRow["case_number"]);
        newVisRow["Credibility Indicator ID"] = "E" + factCheckID.toString();
        factCheckID++;
      }
      newVisRow["Points"] = ".5";                          // You'll need to change this
      newVisRow["Credibility Indicator Name"] = "Waiting for fact-checkers";// You'll need to change this
      newVisRow["Credibility Indicator Category"] = "Evidence";// You'll need to change this
      newVisRow["target_text"] = "nan";// You'll need to change this
      newVisRow["Start"] = triageRow["start_pos"];
      newVisRow["End"] = triageRow["end_pos"];

      visDataArray.push(newVisRow);
      NUM_NFC[id] += 1;
    }
  }
}


function getCenterStyle(num_nfc) {
  switch(num_nfc) {
    case 0:
      return [0, 13, 40, 0, 0, 0, false]
    case 1:
      return [-1, 13, 33, 17, -5, 19, false]
    case 2:
      return [-3, 13, 25, 11, -0, 30, false]
    case 3:
      return [-9.5, 17, 20, 0, 5, 39, false]
    case 4:
      return [-8, 14, 14, -4, 7.5, 48, false]
    case 5:
      return [-23, 26, 13, -19, 20, 56, true]
    default:
      return [-5, 13, 14, 0, 0, 37]
  }
  }
