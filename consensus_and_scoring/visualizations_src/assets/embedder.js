

function embed(link) {
    var frame = $('#embed-frame');
    console.log(frame);
    var tag = "<iframe src='" + link + "' width='100%' height='700'></iframe>"
    frame.html(tag);
}