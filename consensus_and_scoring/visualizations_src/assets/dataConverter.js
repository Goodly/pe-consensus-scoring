
var PILLS_MAP = new Map();


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
function condense(d) {
    if (d.height == 1) {
        var indicators = new Map();
        var indicator;
        for (indicator of d.children) {
            if (indicator.data.data['End'] == -1 || indicator.data.data['Start'] == -1) {
                var name = indicator.data.data["Credibility Indicator Name"] + '-' + indicator.data.data["Credibility Indicator ID"].substring(0, 1);
                if (PILLS_MAP.get(name)) {
                    var points = PILLS_MAP.get(indicator.data.data["Credibility Indicator Name"]);
                    points += parseFloat(indicator.data.data["Points"]);
                } else {
                    PILLS_MAP.set(name, indicator.data.data['Points']);
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
            condense(child);
        }
    }
}



function drawPills(pills_map) {
    var pills_div = $(".pills")[0];
    //console.log(pill_div);
    var div_string = '';
    var entry;
    for (entry of pills_map.entries()) {
        var label = entry[0].substring(0, entry[0].length - 2);
        label += "<br>Score: " + Math.round(parseFloat(entry[1]));
        var categoryInitial = entry[0].substring(entry[0].length - 1, entry[0].length);
        var color = colorFinderPills(categoryInitial);
        var style_string = "style = 'background-color:" +color+"; width:130px; color:#ffffff;height:70px;border-radius: 25px; display: inline-table; margin:3px;padding:5px;vertical-align:bottom;'"
        var pill_div = "<div " + style_string + "><h5 style='font-size:13px;display: table-cell;vertical-align:middle; text-align:center;'>" + label+"</h5></div>";
        console.log(pill_div);
        div_string += pill_div;
    }
    pills_div.innerHTML = div_string;
    
}

setTimeout(function () {
                    drawPills(PILLS_MAP);
                }, 700);