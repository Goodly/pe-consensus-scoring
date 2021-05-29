var TRIAGE_FILE_URL;
var USER_FILE_URL;
var TEXT_FILE_URL;
var DATA_FILE_URL;
var ADJUSTMENT = 0;
var NUM_NFC = 0;

function sortJSONentries(json) {
  var sortArray = []; // an array of arrays
  for (i = 0; i < json.length; i++) {
      if (parseInt(json[i].Start) == -1 || parseInt(json[i].End) == -1 || json[i].Start == "") {
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

function scoreArticle(textFileUrl, dataFileUrl, triageFileUrl, userFileUrl) {
      TEXT_FILE_URL = textFileUrl;
      DATA_FILE_URL = dataFileUrl;
      TRIAGE_FILE_URL = triageFileUrl;
      d3.text(textFileUrl, function(text) {
          document.getElementById("textArticle").innerHTML = text.toString();

          d3.csv(dataFileUrl, function(error, data) {
            if (error) throw error;
            d3.csv(triageFileUrl, function(error, triage_data) {
              if (error) {
                console.log("No triage file found");
                createHighlights(data, triage_data, text.toString());
                hallmark(data);
              } else {
                delete data['columns'];
                createHighlights(data, triage_data, text.toString());
                hallmark(data);
              }
            });

        });
          USER_FILE_URL = userFileUrl;

      });


}




// Pulls the "Needs fact-check" label from the triage data to the vis_data object
function moveFactCheckLabels(triage_data, visDataArray) {
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
    for (triageRow of triage_data) {
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
        ADJUSTMENT = ADJUSTMENT - .5;
        NUM_NFC += 1;
      }
    }
}

function sortTriageEntries(json) {
  var sortArray = []; // an array of arrays
  for (i = 0; i < json.length; i++) {

    // if (json["topic_name"] == "Needs Fact-Check") {
    //   continue; // This would remove it from the article elements view
    // }

    if (json["topic_name"] == "Evidence" || json["topic_name"] == "Reasoning" ||
        json["topic_name"] == "Probability" || json["topic_name"] == "Language") {
        continue;
    }

    if (parseInt(json[i].start_pos) == -1 || parseInt(json[i].end_pos) == -1 || json[i].start_pos == "") {

      continue; // ignore entries where indices are -1 or null
    }
    // [uniqueID, color, index, boolean]

    let uniqueID = json[i]["topic_name"] +'_' + json[i].start_pos + "_" + json[i].end_pos + "_"+json[i]["topic_name"]+ json[i]["case_number"] +  "_triage" ;

    let startEntry = [uniqueID, colorFinderTriage(json[i]), parseInt(json[i].start_pos), true];
    let endEntry = [uniqueID, colorFinderTriage(json[i]), parseInt(json[i].end_pos), false];

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

function createTriageHighlights(json, textString, triage) {
  textArray = textString.split("");  // Splitting the string into an array of strings, one item per character
  var sortedEntries;
  if (triage) {
    sortedEntries = sortTriageEntries(json); // an array highlight arrays, sorted by their indices
  } else {
      sortedEntries = sortUserEntries(json);
  }
  var highlightStack = new FlexArray();

  sortedEntries.forEach((entry) => {  // for each entry, open a span if open or close then reopen all spans if a close
    const index = entry[2];



    if (entry[3]) {
      textArray = openHighlight(textArray, index, entry, highlightStack, 0, false);
      highlightStack.push(entry);
    } else {
      textArray = closeHighlights(textArray, index, highlightStack);
      highlightStack.remove(entry);
      textArray = openHighlights(textArray, index+1, highlightStack, false);
    }
  })

  finalHTML = textArray.join('');
  document.getElementById('textArticle').innerHTML = finalHTML;
  $(".highlight").hover(triageHighlight, triageNormal);
}






function createHighlights(visData, triageData, textString) {
  moveFactCheckLabels(triageData, visData);
  //var textString = document.getElementById('textArticle').innerHTML;
  textArray = textString.split("");  // Splitting the string into an array of strings, one item per character
  var sortedEntries = sortJSONentries(visData); // an array highlight arrays, sorted by their indices
  var highlightStack = new FlexArray();

  sortedEntries.forEach((entry) => {  // for each entry, open a span if open or close then reopen all spans if a close

    const index = entry[2];

    if (entry[3]) {
      textArray = openHighlight(textArray, index, entry, highlightStack, 0, true);
      highlightStack.push(entry);
    } else {
      textArray = closeHighlights(textArray, index, highlightStack);
      highlightStack.remove(entry);
      textArray = openHighlights(textArray, index+1, highlightStack, true);
    }
  });

  finalHTML = textArray.join('');
  document.getElementById('textArticle').innerHTML = finalHTML;
  $(".highlight").hover(highlight, normal);
}

function openHighlight(textArray, index, entry, highlightStack, i, classic) {
  let allIDsBelow = "";
    highlightStack.getArray().forEach((entry) => {
       allIDsBelow = allIDsBelow + entry[0].toString() + "__"; // all the unqiue IDs are separated by dunders
  })
  allIDsBelow = " allIDsBelow='" + allIDsBelow + "'";
  let text = textArray[index-1];
  let uniqueId = entry[0].toString();
  let color = entry[1];
  let name = " name='" + uniqueId + "'";
  var cred_id;
  if (classic) {
    cred_id = " cred_id = '" +uniqueId.substring(0, 2) + "'";
  } else {
    cred_id = " cred_id = '" + uniqueId.split("_")[3] + "'";
  }
  let style = " style= 'border-bottom:1px solid " + color;
  //color['opacity'] = 0.1;
  style = style + "; --color: " + color + "'";
  let highlight = "<span class='highlight' start='"+index+"'" + name + allIDsBelow + cred_id + style + ">";
  textArray[index-1] = text + highlight;
  return textArray;
}

function openHighlights(textArray, index, highlightStack, classic) {
  let text = textArray[index];
  for (var i = 0; i < highlightStack.getSize(); i++) {

    textArray = openHighlight(textArray, index, highlightStack.get(i), highlightStack, i, classic);
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


function triageHighlight(x) {
  var topID = x.toElement.getAttribute("name");
  var colorRGB = x.toElement.style.borderBottomColor;
  var color = colorRGB.match(/\d+/g);                      // split rgb into r, g, b, components
  TRIAGE_DIV.transition().duration(50)
            .style("opacity", .9);
  var divContent;
  var triage = topID.split("_")[topID.split("_").length - 1] == "triage"

  if (triage) {
    var topIDName = topID.split("_")[0];
    if (topIDName != "Needs Fact-Check" && topIDName != "Evidence") {
      topIDName = topIDName.substring(0, topIDName.length -1);
    }
    var cred_id = x.toElement.getAttribute("cred_id");
    cred_id = cred_id[cred_id.length-1];
    divContent = topIDName + " " + cred_id + ", ";
    var allIDsBelow = x.toElement.getAttribute("allidsbelow").split("__");
    var id;
    var cred_ids = [x.toElement.getAttribute("cred_id")];
    for (id of allIDsBelow) {
      if (id == "") {
        continue;
      }
      var id_name = id.split("_")[0];
      cred_id = id.split("_")[3];
      console.log(cred_id);

      var case_number = cred_id.substring(cred_id.length-1, cred_id.length);
      if (!cred_ids.includes(cred_id)) {
        cred_ids.push(cred_id);
      }
      if (id_name != "Needs Fact-Check" && topIDName != "Evidence") {
        id_name = id_name.substring(0, id_name.length - 1);
      }
      if (divContent.includes(id_name)) {
        continue;
      }
      divContent = divContent + id_name + " " + case_number + ", ";
    }
    divContent = divContent.substring(0, divContent.length -2);

    console.log(cred_ids);
    cred_id;

    for (cred_id of cred_ids) {
      $("span[cred_id='"+cred_id+"']").each(function() {
        colorRGB = this.style.borderBottomColor;
        color = colorRGB.match(/\d+/g);
        this.style.setProperty("background-color", "rgba(" + color[0] + "," + color[1] + "," + color[2] + "," + "0.4");
        this.style.setProperty("background-clip", "content-box");
      });
    }
  } else {
      divContent = topID.split("_")[1];
      //We gotta make that score shit....
      score = parseInt(topID.split("_")[2]);
      sunburst = $(".userScore");
      sunburst.text("Your score for this highlight: "+ score);
      sunburst.css("border-color", colorRGB);
      sunburst.css("width", "23ch");
  }



  var words = divContent.split(" ");
  var longest = words.sort(
    function (a, b) {
      return b.length - a.length;
    }
  )[0];
  var max_width = Math.max(17, longest.length);

  TRIAGE_DIV.style("width", function() {
      if (divContent.length < 17) {
          return divContent.length.toString() + "ch";
      } else {
          return max_width.toString() +"ch";
      }
  });

  var box_x = $(".p-article")[0].getBoundingClientRect().x - 13*max_width;
  var box_y = event.clientY;
  TRIAGE_DIV.style("min-height", "1ch");
  TRIAGE_DIV.style("position", "absolute");
  TRIAGE_DIV.style("height", "fit-content");
  TRIAGE_DIV.style("left", box_x + "px")
            .style("top", box_y + "px")
            .html(divContent);
  x.toElement.style.setProperty("background-color", "rgba(" + color[0] + "," + color[1] + "," + color[2] + "," + "0.4");
  x.toElement.style.setProperty("background-clip", "content-box");
}


function highlight(x) {
  var topID = x.toElement.getAttribute("name");
  var cred_id = x.toElement.getAttribute("cred_id");
  var color = x.toElement.style.borderBottomColor;      // grab color of border underline in rgb triage
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


function triageNormal(x) {
    TRIAGE_DIV.transition()
    .delay(5)
    .duration(200)
    .style("opacity", 0);
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
      .html(("?"));
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
