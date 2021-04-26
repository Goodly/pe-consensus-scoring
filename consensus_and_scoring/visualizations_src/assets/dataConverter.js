



//Add dummy data so that the data has the correct nodes to form a tree.
function addDummyData(data) {
  var categories = new Set([]);
  var i = 0;

  //Get all categories that are non-empty.
  data.forEach((highlight) => {
    if (highlight["Credibility Indicator Category"]) {
      categories.add(highlight["Credibility Indicator Category"]);
      i++;
    }
  });
  //Add all categories as nodes to the data with parent as CATEGORIES.
  categories.forEach((category) => {
    data[i] = {"Credibility Indicator Category": "CATEGORIES", "Credibility Indicator Name": category};
    i ++;
  })



  //Add root nodes.
  data[i] = {"Credibility Indicator Category": undefined, "Credibility Indicator Name": "CATEGORIES"};
  return data;
}

//Convert data to a hierarchical format.
function convertToHierarchy(data) {
  //Stratify converts flat data to hierarchal data.

  var stratify = d3.stratify()
    .id(d => d["Credibility Indicator Name"])
    .parentId(d => d["Credibility Indicator Category"])
    (data);
  //Hierarchy converts data to the same format that the D3 code expects.
  return d3.hierarchy(stratify);
}


/** Takes a heirarchical json file and converts it into a tree with unique branches
and unique leaves.
@param d: a heirarchicical json file outputted by convertToHeirarchy
*/
function condense(d, pills_map) {
    if (d.height == 1) {
        var indicators = new Map();
        var indicator;
        for (indicator of d.children) {
            if (false) {
                var name = indicator.data.data["Credibility Indicator Name"] + '-' + indicator.data.data["Credibility Indicator ID"].substring(0, 1);
                if (pills_map.get(name)) {
                    var points = pills_map.get(name);
                    points += parseFloat(indicator.data.data["Points"]);
                    pills_map.set(name, points);
                } else {
                    pills_map.set(name, parseFloat(indicator.data.data['Points']));
                }
            } else if (indicators.get(indicator.data.data["Credibility Indicator Name"])) {
                json = indicators.get(indicator.data.data["Credibility Indicator Name"]).data.data;
                json["Points"] = parseFloat(json.Points) + parseFloat(indicator.data.data["Points"]);
            } else {
                //console.log(indicator.data.data["Credibility Indicator Name"]);
                indicators.set(indicator.data.data["Credibility Indicator Name"], indicator);
            }
        }
        //console.log(indicators);
        var newChildren = Array.from(indicators.values());
        d.children = newChildren;
        d.data.children = newChildren;
        //d.children = newChildren;

    } else {
        var child;
        for (child of d.children) {
            condense(child, pills_map);
        }
    }
}



function drawPills(pills_map) {
    // var pills_div = $(".pills")[0];
    var pills_div = document.getElementsByClassName('sunburst')[0];
    let new_div = document.createElement('div');
    new_div.className = 'pill_container';
    var div_string = '';
    var entry;
    for (entry of pills_map.entries()) {
        var label = entry[0].substring(0, entry[0].length - 2);
        var categoryInitial = entry[0].substring(entry[0].length - 1, entry[0].length);
        var score = entry[1];
        var color = colorFinderPills(categoryInitial);
        var style_string = "style = 'background-color:" +color+"; width:130px; color:#ffffff;height:45px;border-radius: 25px; display: inline-table; margin:3px;padding:5px;vertical-align:bottom;  transform: translate(40%, 0%)'"
        var class_string = "class='" + entry[0] + "'";
        var java_string = "onmouseover='pillMouseover(" + Math.round(score) + ");' onmouseleave='pillMouseleave();'";
        var pill_div = "<div " + class_string + " " + style_string + " " + java_string + "><h5 style='font-size:13px;display: table-cell;vertical-align:middle; text-align:center;'>" + label+"</h5></div>";

        div_string += pill_div;
    }
    new_div.innerHTML = div_string;
    pills_div.appendChild(new_div);
    $(".pill_container").css({position: 'relative', top:395});
    // pills_div.innerHTML = div_string;

}






function pillMouseover(score) {
    SVG.selectAll(".center-text").style('display', 'none');
    SVG.append("text")
        .attr("class", "center-text")
        .attr("x", 0)
        .attr("y", 13)
        .style("font-size", 40)
        .style("text-anchor", "middle")
        .html((score));
}

function pillMouseleave() {
    SVG.selectAll(".center-text").style('display', 'none');
    SVG.append("text")
        .attr("class", "center-text")
        .attr("x", 0)
        .attr("y", 13)
        .style("font-size", 40)
        .style("text-anchor", "middle")
        .html((totalScore));
}
