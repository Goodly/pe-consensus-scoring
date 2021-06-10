async function moveHallmarks() {
  var item;
  for (item of listofarticles) {
      var divID = item['sha256'];
      var hallmark = document.querySelector("svg[articleID='" + divID +"']");
      waitForElementToDisplay("#"+divID, "svg[articleID='" + divID +"']", 100, item);
  }
}


async function removeHallmarks() {
  var id;
  for (id of SVG_IDS) {
     d3.select("body").select("svg").remove();
  }
d3.select("body").select("svg").remove();

}




function waitForElementToDisplay(divSelector, hallmarkSelector, time, item) {
        if($(divSelector)[0]!=null && document.querySelector(hallmarkSelector)!=null) {
            var hallmark = document.querySelector(hallmarkSelector);
            var element = $(divSelector)[0];
            var box = element.getBoundingClientRect();
            var box_y = box.top;
            hallmark.style.position = "absolute";
            hallmark.style.left = "70%";
            hallmark.style.top = box_y;
            item.credibilityScore = parseInt(hallmark.getAttribute("score"));
            return;
        }
        else {
            setTimeout(function() {
                waitForElementToDisplay(divSelector, hallmarkSelector, time, item);
            }, time);
        }
    }
