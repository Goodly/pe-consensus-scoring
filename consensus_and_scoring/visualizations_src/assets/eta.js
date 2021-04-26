


function runPyScript(article_sha256, user_id, form, e){
    var jqXHR = $.ajax({
        type: "POST",
        url: "http://127.0.0.1:5000/login",
        async: false,
        data: { article_sha256: article_sha256, user_id: user_id, form:form},
        crossDomain: true,
        success: function(data, textStatus) {
          console.log('I think it worked...?')
        }
    });

    return false;
}
