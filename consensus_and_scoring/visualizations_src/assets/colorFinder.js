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
  } else {
      return d3.rgb(255, 180, 0);
  }
}

function colorFinderForm(jsonLine) {
    if (jsonLine["Credibility Indicator Category"] === "Arguments") {
    return d3.rgb(239, 92, 84);
  } else if (jsonLine["Credibility Indicator Category"] === "Quoted Sources") {
    return d3.rgb(0, 165, 150);
  } else if (jsonLine["Credibility Indicator Category"] === "Assertions") {
      return d3.rgb(0, 191, 255);
  } else if (jsonLine["Credibility Indicator Category"] == "Needs Fact-Check") {
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
  } else {
      return d3.rgb(255, 180, 0);
  }
}