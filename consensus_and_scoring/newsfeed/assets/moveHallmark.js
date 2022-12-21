async function moveHallmark(articleIndex) {
    const entry = LIST_OF_ARTICLES[articleIndex];
    var divID = entry['sha256'];
    waitForElementToDisplay("#"+divID, "svg[articleID='" + divID +"']", 100, entry);
}

function waitForElementToDisplay(divSelector, hallmarkSelector, time, item) {
        if($(divSelector)[0]!=null && document.querySelector(hallmarkSelector)!=null) {
            var hallmark = document.querySelector(hallmarkSelector);
            var element = $(divSelector)[0];
            var box = element.getBoundingClientRect();
            var box_y = box.top;
            hallmark.style.position = "absolute";
            hallmark.style.left = "70%";
            hallmark.style.top = box_y + window.pageYOffset;
            item.credibilityScore = parseInt(hallmark.getAttribute("score"));
            return;
        }
        else {
            setTimeout(function() {
                waitForElementToDisplay(divSelector, hallmarkSelector, time, item);
            }, time);
        }
    }
