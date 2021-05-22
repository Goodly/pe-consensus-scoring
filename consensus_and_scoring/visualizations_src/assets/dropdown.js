window.addEventListener("DOMContentLoaded", function () {
  var button_one = document.getElementById("button_one");
  var button_two = document.getElementById("button_two");
  var button_three = document.getElementById("button_three");
  var input = document.getElementById('userid')
  var content = document.getElementsByClassName("dropdown-content")[0];
  // HARD CODED
  var userIDs = ['1234', '5678', '9012']
  //HARD CODED

  var articleElements = document.getElementsByClassName('hide')[0];
  var yourCont = document.getElementsByClassName('hide')[1];

  button_one.onclick = function() {
    $("#popup").css("opacity", 1);
    classic = true;
    content.style.display = "none";
    articleElements.setAttribute('class', 'hide');
    yourCont.setAttribute('class', 'hide');
    $("#chart").css("opacity", 1);
    // document.querySelector('#userid').setAttribute('class', 'hidden');
    // document.querySelector('#errormsg').setAttribute('class', 'hidden');
    sunburst = $(".userScore");
    sunburst.css("display", "none");
    this.style.opacity = 1;
    button_two.style.opacity = 0.6;
    button_three.style.opacity = 0.6;
    d3.text(TEXT_FILE_URL, function(text) {
      document.getElementById("textArticle").innerHTML = text.toString();
      d3.csv(DATA_FILE_URL, function(error, data) {
        if (error) throw error;
        createHighlights(data, text.toString());
      });
    });
  }

  //Your Contribution
  // input.addEventListener("keyup", e => { // redoing individual contribution functionality
  //   if (e.keyCode === 13) {
  //     console.log(input.value);
  //     var userid = input.value;
  //
  //     if (!userIDs.includes(userid)) {
  //       console.log("Invalid USERID");
  //       document.querySelector('#errormsg').setAttribute('class', 'shown');
  //     } else {
  //       classic = false;
  //       visualizationOn = false;
  //       $("#chart").css("opacity", .2);
  //       $("#popup").css("opacity", .5);
  //       $("myModal").hide();
  //       document.querySelector('#errormsg').setAttribute('class', 'hidden');
  //       // Implement USERID lookup here.
  //       sunburst = $(".userScore");
  //       sunburst.slideDown();
  //       //sunburst.css("display", "block");
  //
  //
  //       d3.text(TEXT_FILE_URL, function(text) {
  //         document.getElementById("textArticle").innerHTML = text.toString();
  //         d3.csv(USER_FILE_URL, function(error, data) {
  //           if (error) {
  //             console.log('No user file found');
  //           } else {
  //             createFormHighlights(data, text.toString(), false);
  //           }
  //           });
  //       });
  //     }
  //     input.value = "";
  //   }
  // });


  button_two.onclick = function() {
    //document.querySelector('#userid').setAttribute('class', 'userform');
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

    content.style.display = "block";
    yourCont.setAttribute('class', 'show');
    articleElements.setAttribute('class', 'hide');
    this.style.opacity = 1;
    button_one.style.opacity = 0.6;
    button_three.style.opacity = 0.6;


    // Gray out hallmark, remove functionality
    classic = false;
    visualizationOn = false;
    $("#chart").css("opacity", .2);
    $("#popup").css("opacity", .5);
    $("#myModal").hide();
    $("#popup").html("?");
    sunburst = $(".userScore");
    sunburst.css("display", "none");
  }

  //Article Elements
  button_three.onclick = function() {
    classic = false;
    visualizationOn = false;
    $("#chart").css("opacity", .2);
    $("#popup").css("opacity", .5);
    $("#myModal").hide();
    $("#popup").html("?");
    content.style.display = "block";
    articleElements.setAttribute('class', 'show');
    yourCont.setAttribute('class', 'hide');
    // document.querySelector('#userid').setAttribute('class', 'hidden'); redoing individual contribution functionality
    // document.querySelector('#errormsg').setAttribute('class', 'hidden');
    sunburst = $(".userScore");
    sunburst.css("display", "none");
    this.style.opacity = 1;
    button_one.style.opacity = 0.6;
    button_two.style.opacity = 0.6;
    var article_sha256;




    d3.text(TEXT_FILE_URL, function(text) {
      document.getElementById("textArticle").innerHTML = text.toString();
      d3.csv(FORM_FILE_URL, function(error, data) {
        if (error) throw error;
        createFormHighlights(data, text.toString(), true);
      });
    });
  }

});
