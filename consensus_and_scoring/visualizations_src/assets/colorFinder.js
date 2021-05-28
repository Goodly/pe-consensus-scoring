function colorFinder(jsonLine) {
  //The children node colors are based on the colors of their parents.
  if (jsonLine["Credibility Indicator Category"] === "Reasoning") {
    return d3.rgb(239, 92, 84);
  } else if (jsonLine["Credibility Indicator Category"] === "Evidence") {
    return d3.rgb(0, 165, 150);
  } else if (jsonLine["Credibility Indicator Category"] === "Probability") {
      return d3.rgb(0, 191, 255);
  } else if (jsonLine["Credibility Indicator Category"] == "Language") {
      return d3.rgb(43, 82, 230);
  } else if (jsonLine["Credibility Indicator Category"] == "Holistic"){
      return d3.rgb(255, 180, 0);
  } else if (jsonLine["Credibility Indicator Category"] == "Sourcing") {
      return d3.rgb(201, 87, 198)
  } else {
    return d3.rgb(255, 255, 255);
  }
}

function colorFinderTriage(jsonLine) {
    if (jsonLine["topic_name"] === "Arguments") {
    return d3.rgb(239, 92, 84);
  } else if (jsonLine["topic_name"] === "Quoted Sources") {
    return d3.rgb(0, 165, 150);
  } else if (jsonLine["topic_name"] === "Assertions") {
      return d3.rgb(0, 191, 255);
  } else if (jsonLine["topic_name"] == "Needs Fact-Check") {
      return d3.rgb(43, 82, 230);
  }
}



function colorFinderPills(categoryInitial) {
    if (categoryInitial == 'R') {
        return "#EF7559";
    } else if (categoryInitial == 'E') {
        return "#57C1AE"
    } else if (categoryInitial == 'P') {
        return "#76BCE2";
    } else if (categoryInitial == "L") {
        return "#4B5FB2";
    } else {
        return "#FDB515";
    }
}
/*
    Given a category, output the corresponding color


*/
function colorFinderCategory(category) {
  if (category === "Reasoning") {
    return d3.rgb(239, 92, 84);
  } else if (category === "Evidence") {
    return d3.rgb(0, 165, 150);
  } else if (category == "Probability") {
      return d3.rgb(0, 191, 255);
  } else if (category == "Language") {
      return d3.rgb(43, 82, 230);
  } else if (category == "Holistic"){
      return d3.rgb(255, 180, 0);
  } else if (category == "Sourcing") {
    return d3.rgb(201, 87, 198)
  } else {
    return d3.rgb(255, 255, 255);
  }
}
