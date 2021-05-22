var FORM_FILE_URL;
var USER_FILE_URL;
var TEXT_FILE_URL;
var DATA_FILE_URL;

function sortJSONentries(json) {
  var sortArray = []; // an array of arrays
  for (i = 0; i < json.length; i++) {
      if (parseInt(json[i].Start) == -1 || parseInt(json[i].End) == -1 || json[i].Start == "") {
        //console.log(json[i]);
        continue; // ignore entries where indices are -1 or null]
      }
    // [uniqueID, color, index, boolean]

    let uniqueID = json[i]["Credibility Indicator ID"] +'-' + json[i]["Credibility Indicator Name"] + "-" + json[i].Start + "-" + json[i].End;

    let startEntry = [uniqueID, colorFinder(json[i]), parseInt(json[i].Start), true];
    let endEntry = [uniqueID, colorFinder(json[i]), parseInt(json[i].End), false];

    sortArray.push(startEntry);
    sortArray.push(endEntry);
  }
  sortArray = sortArray.sort(highlightSort); // sorting all entries by their indices

  return sortArray;
}

function scoreArticle(textFileUrl, dataFileUrl, formFileUrl, userFileUrl) {
      TEXT_FILE_URL = textFileUrl;
      DATA_FILE_URL = dataFileUrl;
      d3.text(textFileUrl, function(text) {
          document.getElementById("textArticle").innerHTML = text.toString();

          d3.csv(dataFileUrl, function(error, data) {
            if (error) throw error;
            d3.csv(formFileUrl, function(error, form_data) {
              if (error) {
                console.log("No form file found");
                createHighlights(data, text.toString());
                hallmark(data)
              } else {
                delete data['columns'];
                moveFactCheckLabels(form_data, data);
                createHighlights(data, text.toString());
                data.splice(data.length-2, 1);
                hallmark(data);
              }
            });

        });
          FORM_FILE_URL = formFileUrl;
          USER_FILE_URL = userFileUrl;

      });


}


// Mutates data


function moveFactCheckLabels(form_data, visDataArray) {

    for (rowIndex in form_data) {
      if (form_data[rowIndex]["Credibility Indicator Category"] == "Needs Fact-Check") {
        // data[rowIndex]
        var row = Object.assign({}, form_data[rowIndex]);

        delete row[""]
        delete row["Case Number"]

        row["Points"] = ".5";
        row["Credibility Indicator ID"] = "E26";
        row["Credibility Indicator Name"] = "Waiting for fact-checkers";
        row["Credibility Indicator Category"] = "Evidence";
        row["target_text"] = "nan";
        Object.defineProperty(row, 'Article ID',
            Object.getOwnPropertyDescriptor(row, 'Article sha256'));
        delete row['Article sha256'];
        //console.log(row);
        visDataArray.push(row);
        //console.log(visDataArray[visDataArray.length-1]);
      }
    }
}

function sortFormEntries(json) {
  var sortArray = []; // an array of arrays
  for (i = 0; i < json.length; i++) {

      if (parseInt(json[i].Start) == -1 || parseInt(json[i].End) == -1 || json[i].Start == "") {

        continue; // ignore entries where indices are -1 or null
      }
    // [uniqueID, color, index, boolean]

    let uniqueID = json[i]["Credibility Indicator Category"] +'_' + json[i].Start + "_" + json[i].End + "_form";

    let startEntry = [uniqueID, colorFinderForm(json[i]), parseInt(json[i].Start), true];
    let endEntry = [uniqueID, colorFinderForm(json[i]), parseInt(json[i].End), false];

    sortArray.push(startEntry);
    sortArray.push(endEntry);
  }
  sortArray = sortArray.sort(highlightSort); // sorting all entries by their indices
  return sortArray;
}


function sortUserEntries(json) {
  var sortArray = [];
  for (i = 0; i < json.length; i++) {
      if (parseInt(json[i].Start) == -1 || parseInt(json[i].End) == -1 || (json[i].Start == 0 && json[i].End == 0)) {
        continue; // ignore entries where indices are -1 or null
      }
    // [uniqueID, color, index, boolean]

//    let uniqueID = json[i]["question_text"] +'-'+json[i]["answer_text"] +"-"+ json[i].start_pos + "-" + json[i].end_pos + "-user";
    let uniqueID = json[i]["Credibility Indicator Category"] + "_" +
                    json[i]['Credibility Indicator Name'] + "_" +
                    json[i]["Point Recommendation"] + "_" + json[i]["Start"] +
                    "_" + json[i]["End"]
    color = colorFinderCategory(json[i]["Credibility Indicator Category"])
    let startEntry = [uniqueID, color, parseInt(json[i].Start), true];
    let endEntry = [uniqueID, color, parseInt(json[i].End), false];

    sortArray.push(startEntry);
    sortArray.push(endEntry);
  }
  sortArray = sortArray.sort(highlightSort); // sorting all entries by their indices

  return sortArray;
}

function createFormHighlights(json, textString, form) {
  textArray = textString.split("");  // Splitting the string into an array of strings, one item per character
  var sortedEntries;
  if (form) {
    sortedEntries = sortFormEntries(json); // an array highlight arrays, sorted by their indices
  } else {
      sortedEntries = sortUserEntries(json);
  }
  var highlightStack = new FlexArray();

  sortedEntries.forEach((entry) => {  // for each entry, open a span if open or close then reopen all spans if a close
    const index = entry[2];

    if (entry[3]) {
      textArray = openHighlight(textArray, index, entry, highlightStack, 0);
      highlightStack.push(entry);
    } else {
      textArray = closeHighlights(textArray, index, highlightStack);
      highlightStack.remove(entry);
      textArray = openHighlights(textArray, index+1, highlightStack);
    }
  })

  finalHTML = textArray.join('');
  document.getElementById('textArticle').innerHTML = finalHTML;
  $(".highlight").hover(formHighlight, formNormal);
}






function createHighlights(json, textString) {
  //var textString = document.getElementById('textArticle').innerHTML;
  textArray = textString.split("");  // Splitting the string into an array of strings, one item per character
  var sortedEntries = sortJSONentries(json); // an array highlight arrays, sorted by their indices
  var highlightStack = new FlexArray();

  sortedEntries.forEach((entry) => {  // for each entry, open a span if open or close then reopen all spans if a close

    const index = entry[2];

    if (entry[3]) {
      textArray = openHighlight(textArray, index, entry, highlightStack, 0);
      highlightStack.push(entry);
    } else {
      textArray = closeHighlights(textArray, index, highlightStack);
      highlightStack.remove(entry);
      textArray = openHighlights(textArray, index+1, highlightStack);
    }
  })

  finalHTML = textArray.join('');
  document.getElementById('textArticle').innerHTML = finalHTML;
  $(".highlight").hover(highlight, normal);
}

function openHighlight(textArray, index, entry, highlightStack, i) {
  let allIDsBelow = "";
    highlightStack.getArray().forEach((entry) => {
       allIDsBelow = allIDsBelow + entry[0].toString() + "__"; // all the unqiue IDs are separated by dunders
  })
  allIDsBelow = " allIDsBelow='" + allIDsBelow + "'";
  let text = textArray[index-1];
  let uniqueId = entry[0].toString();
  let color = entry[1];
  let name = " name='" + uniqueId + "'";
  let cred_id = " cred_id = '" +uniqueId.substring(0, 2) + "'";
  let style = " style= 'border-bottom:1px solid " + color;
  color['opacity'] = 0.1;
  style = style + "; --color: " + color + "'";
  let highlight = "<span class='highlight' start='"+index+"'" + name + allIDsBelow + cred_id + style + ">";
  textArray[index-1] = text + highlight;
  return textArray;
}

function openHighlights(textArray, index, highlightStack) {
  let text = textArray[index];
  for (var i = 0; i < highlightStack.getSize(); i++) {

    textArray = openHighlight(textArray, index, highlightStack.get(i), highlightStack, i);
  }
  // highlightStack.getArray().forEach((entry) => {
  //   textArray = openHighlight(textArray, index, entry);
  // })
  return textArray;
}

function closeHighlights(textArray, index, highlightStack) {
  let text = textArray[index];
  let closeSpans = '';
  for (var i = 0; i < highlightStack.getSize(); i++) {
    closeSpans += "</span>";
  }

  textArray[index] = text + closeSpans;
  return textArray;
}


function formHighlight(x) {
  var topID = x.toElement.getAttribute("name");
  var colorRGB = x.toElement.style.borderBottomColor;
  var color = colorRGB.match(/\d+/g);                      // split rgb into r, g, b, components
  DIV.transition().duration(50)
            .style("opacity", .9);
  var divContent;
  var form = topID.split("_")[topID.split("_").length - 1] == "form"

  if (form) {
    divContent = topID.split("_")[0];

  } else {
      divContent = topID.split("_")[1];
      //We gotta make that score shit....
      score = parseInt(topID.split("_")[2]);
      sunburst = $(".userScore");
      sunburst.text("Your score for this highlight: "+ score);
      sunburst.css("border-color", colorRGB);
      sunburst.css("width", "23ch");
  }


  DIV.style("position", "fixed");
  var width;
  DIV.style("width", function() {
      if (divContent.length < 30) {
          width = (divContent.length * 10);
          return width.toString() +"px";
      } else {
          width = divContent.length + 5
          return width.toString() +"px";
      }
  });

  var box_x = $(".p-article")[0].getBoundingClientRect().x - width - 40;
  var box_y = event.clientY;
  DIV.style("height", "auto");
  DIV.style("left", box_x + "px")
            .style("top", box_y + "px")
            .html(divContent);
  x.toElement.style.setProperty("background-color", "rgba(" + color[0] + "," + color[1] + "," + color[2] + "," + "0.4");
  x.toElement.style.setProperty("background-clip", "content-box");
}


function highlight(x) {
  var topID = x.toElement.getAttribute("name");
  var cred_id = x.toElement.getAttribute("cred_id");
  var color = x.toElement.style.borderBottomColor;      // grab color of border underline in rgb form
  var color = color.match(/\d+/g);                      // split rgb into r, g, b, components
  var allIds = x.toElement.getAttribute("allIDsBelow").concat("__" + topID)
  if (allIds.substring(0,1) == ' ') {
      allIds = allIds.substring(1);
  }
  allIds = allIds.split("__");
  highlightManyHallmark(allIds, ROOT);
  $("span[cred_id='"+cred_id+"']").each(function() {
    this.style.setProperty("background-color", "rgba(" + color[0] + "," + color[1] + "," + color[2] + "," + "0.25");
    this.style.setProperty("background-clip", "content-box");
  });


}


function formNormal(x) {
    DIV.transition()
        .duration(600)
        .style("opacity",0);
    DIV.style("position", "absolute");
    var allSpans = document.getElementsByTagName('span');
    for (var i = 0; i < allSpans.length; i++) {
      allSpans[i].style.setProperty("background-color", "transparent");
    }
    sunburst = $(".userScore");
    sunburst.text("Your score for this highlight:");
    sunburst.css("border-color", "#FFFFFF")

}

// A function which returns all our background colors back to normal.
// Needs fix to optimize, currently loops through all spans.
function normal(x) {

    //resetVis(ROOT);
    resetHallmark(ROOT);
    PSEUDOBOX.transition()
        .delay(300)
        .duration(600)
        .style("opacity", 0)
  var allSpans = document.getElementsByTagName('span');
  for (var i = 0; i < allSpans.length; i++) {
    allSpans[i].style.setProperty("background-color", "transparent");
  }
}

function resetHallmark() {
    d3.selectAll("path")
        .transition()
        .delay(300)
        .duration(800)
        .attr('stroke-width',2)
        .style("opacity", function(d) {
            if (d) {
                if (d.height == 1) {
                } else {
                    return 0;
                }
            } else {
                console.log("we caught a bird y'all");
            }
        })
    d3.selectAll("path")
        .transition()
        .delay(1000)
        .attr('stroke-width',2)
        .style("opacity", function(d) {
            if (d) {
                 if (d.height == 1) {
                } else {
                    return 0;
                }
            } else {
                console.log("nothing to see here");
            }
        })
    SVG.selectAll(".center-text").style('display', 'none');
    SVG.append("text")
        .attr("class", "center-text")
        .attr("x", 0)
        .attr("y", 13)
        .style("font-size", 40)
        .style("text-anchor", "middle")
        .html((totalScore));
}


function highlightManyHallmark(idArray, d) {
    var id;
    var pathList = [];
    var catList = [];
    var indicators = "";
    var pointsGained = 0;
    for (id of idArray) {
        if (id != "") {
            var category;
            for (category of d.children) {

                var categoryName = category.data.data['Credibility Indicator Name'];
                var catPath = nodeToPath.get(category);
                d3.select(catPath)
                .transition()
                .style("opacity", .5);
                if (id.substring(0, 1) == categoryName.substring(0, 1)) {
                    catList = catList.concat(catPath);
                    var indicator;
                    for (indicator of category.children) {
                        var indicatorID = indicator.data.data['Credibility Indicator ID'];
                        var indicatorName = indicator.data.data["Credibility Indicator Name"];
                        var path = nodeToPath.get(indicator);
                        d3.select(path)
                        .transition()
                        .style("display", "block")
                        .style("opacity", .5);
                        var nameFromElement = id.substring(3);
                        nameFromElement = nameFromElement.replace(/[0-9]|[-]/g, '');
                        if (nameFromElement == "Waiting for factcheckers") {
                          nameFromElement = "Waiting for fact-checkers";
                        }
                        //console.log(indicatorName);
                        //console.log(nameFromElement);
                        if (indicatorName == nameFromElement) {
                            pathList = pathList.concat(path);
                            var score = scoreSum(indicator);
                            pointsGained += score;
                            if (!indicators.includes(indicatorName)) {
                                indicators += indicatorName + ", ";
                            }
                        }
                    }
                }
            }
        }
    }

    indicators = indicators.substring(0, indicators.length - 2);
    var c;
    for (c of catList) {
        d3.select(c)
        .transition()
        .style("display", "block")
        .style("opacity", 1)
        .duration(200);
    }
    var p;
    for (p of pathList) {
        d3.select(p)
        .transition()
        .style("display", "block")
        .style("opacity", 1);
    }

    var element = document.getElementById('chart');
    var position = element.getBoundingClientRect();
    x = position.left + 35;
    y = position.top + 330;

    PSEUDOBOX.transition()
        .duration(200)
        .style("opacity", .9);
    PSEUDOBOX.html(indicators)
        .style("left", (x) + "px")
        .style("top", (y) + "px")
        .style("width", "min-content")
        .style("height", "min-content");


    SVG.selectAll(".center-text").style('display', 'none');
    if (indicators == "Waiting for fact-checkers") {
      SVG.append("text")
      .attr("class", "center-text")
      .attr("x", 0)
      .attr("y", 13)
      .style("font-size", 40)
      .style("text-anchor", "middle")
      .html((""));
    } else {
    SVG.append("text")
    .attr("class", "center-text")
    .attr("x", 0)
    .attr("y", 13)
    .style("font-size", 40)
    .style("text-anchor", "middle")
    .html((pointsGained));
}

}



function highlightHallmark(id) {

    d3.selectAll("path").transition().each(function(d) {
    if (d.height == 2) {
        var category;
        for (category of d.children) {
            var categoryName = category.data.data['Credibility Indicator Category'];
            if (id.substring(0, 1) == categoryName.substring(0, 1)) {
                var indicator;
                for (indicator of category.children) {
                    var indicatorName = indicator.data.data['Credibility Indicator Name']
                    var indices = indicator.data.data["Start"] + "-"+indicator.data.data["End"];
                    var nameFromElement = id.substring(3);
                    nameFromElement = nameFromElement.replace(/[0-9]|[-]/g, '');
                    if (nameFromElement == indicatorName) {

                        var path = nodeToPath.get(indicator);
                        d3.select(path)
                        .transition()
                        .style("display", "block")
                        .style("opacity", 1)
                        .duration(200);

                        var element = document.getElementById('chart');
                        var position = element.getBoundingClientRect();
                        x = position.left + 35;
                        y = position.top + 280;
                        var pointsGained = scoreSum(indicator);
                        SVG.selectAll(".center-text").style('display', 'none');
                        SVG.append("text")
                            .attr("class", "center-text")
                            .attr("x", 0)
                            .attr("y", 13)
                            .style("font-size", 40)
                            .style("text-anchor", "middle")
                            .html((pointsGained));
                        PSEUDOBOX.transition()
                            .duration(200)
                            .style("display", "block")
                            .style("opacity", .9);
                        PSEUDOBOX.html(indicator.data.data['Credibility Indicator Name'])
                            .style("left", (x) + "px")
                            .style("top", (y) + "px")
                            .style("width", function() {
                                if (indicator.data.data['Credibility Indicator Name'].length < 18) {
                                    return "90px";
                                } else {
                                    return "180px";
                                }
                            })

                    } else {
                        var path = nodeToPath.get(indicator);
                        d3.select(path)
                        .transition()
                        .style("display", "block")
                        .style("opacity", .5)
                        .duration(200);

                    }
                }

            } else {

                var path = nodeToPath.get(category);
                d3.select(path)
                .transition()
                .style("opacity", 0.5)
                .duration(300)

            }


        }
    }
})
}
