var HOLISTIC_MAP;


// Remove anything that might be used as a multiple ROOT
// Mutates data
function clean(data) {
  for(var i in data){
    if(data[i]["Article ID"]==""){
      data.splice(i,1);
      break;
    }
  }
}

//Add dummy data so that the data has the correct nodes to form a tree.
function addDummyData(data) {
  HOLISTIC_MAP = new Map();
  var newData = [];

  var line;
  for (line of data) {
    newData.push(line);
  }
  // for (line of data) {
  //   //console.log(line);
  //   if (line["End"] == "-1" || line["Start"] == "-1") {
  //     if (HOLISTIC_MAP.has(line["Credibility Indicator Name"])) {
  //       var score = HOLISTIC_MAP.get(line["Credibility Indicator Name"]) + parseFloat(line["Points"]);
  //       HOLISTIC_MAP.set(line["Credibility Indicator Name"], score);
  //     } else {
  //       HOLISTIC_MAP.set(line["Credibility Indicator Name"], parseFloat(line["Points"]));
  //     }
  //   } else {
  //     newData.push(line);
  //   }
  // }




  var categories = new Set([]);
  var i = 0;
  //Get all categories that are non-empty.
  newData.forEach((highlight) => {
    if (highlight["Credibility Indicator Category"]) {
      categories.add(highlight["Credibility Indicator Category"]);
      i ++;
    }
  });
  //Add all categories as nodes to the data with parent as CATEGORIES.
  categories.forEach((category) => {
    newData[i] = {"Credibility Indicator Category": "CATEGORIES", "Credibility Indicator Name": category};
    i ++;
  })



  //Add root nodes.
  newData[i] = {"Credibility Indicator Category": undefined, "Credibility Indicator Name": "CATEGORIES"};


  return newData;
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
@param data: a heirarchicical json file outputted by convertToHeirarchy
*/
function condense(d) {
    if (d.height == 1) {
        var indicators = new Map();
        var niceIndicators = new Map();
        var naughtyIndicators = new Map();
        var indicator;
        for (indicator of d.children) {
          if (indicators.get(indicator.data.data["Credibility Indicator Name"])) {
              json = indicators.get(indicator.data.data["Credibility Indicator Name"]).data.data;
              json["Points"] = parseFloat(json.Points) + parseFloat(indicator.data.data["Points"]);
              if (indicator.data.data["Start"] != -1 && indicator.data.data["End"] != -1) {
                json["Start"] = indicator.data.data["Start"];
                json["End"] = indicator.data.data["End"];
                json["Credibility Indicator ID"] = indicator.data.data["Credibility Indicator ID"];
              }
          } else {
              indicators.set(indicator.data.data["Credibility Indicator Name"], indicator);
          }
        }

        var newChildren = Array.from(indicators.values());
        d.children = newChildren;
        d.data.children = newChildren;
    } else {
        var child;
        for (child of d.children) {
            condense(child);
        }
    }
}
