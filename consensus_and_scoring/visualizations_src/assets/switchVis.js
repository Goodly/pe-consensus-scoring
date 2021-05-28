$(document).ready(function () {
    $("#switchOptions").change(function () {
        var val = $(this).val();
        if (val == "alg") {
          d3.text(TEXT_FILE_URL, function(text) {
            document.getElementById("textArticle").innerHTML = text.toString();
            d3.csv(DATA_FILE_URL, function(error, data) {
              if (error) throw error;
              createHighlights(data, text.toString());
            });
          });
        } else if (val == "user") {
           d3.text(TEXT_FILE_URL, function(text) {
             document.getElementById("textArticle").innerHTML = text.toString();
             d3.csv(USER_FILE_URL, function(error, data) {
               if (error) {
                 console.log('No user file found');
               } else {
                 createFormHighlights(data, text.toString(), false);
               }
             });
           });
        } else if (val == "form") {
            d3.text(TEXT_FILE_URL, function(text) {
            document.getElementById("textArticle").innerHTML = text.toString();
            d3.csv(FORM_FILE_URL, function(error, data) {
              if (error) {
                console.log("no form file found");
              } else {
                createFormHighlights(data, text.toString(), true);   //might not be able to use formHighlights for the user file...have to see
              }
            });
          });
        }
    });
});
