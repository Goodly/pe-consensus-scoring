$(window).scroll(function(){
    //console.log('scrolling');
    var header = $(".header-display");
    var base_grid = $(".base-grid");
    header_top = header[0].getBoundingClientRect().top;
    base_grid_top = base_grid[0].getBoundingClientRect().top;
    var footer = $(".footer-span");
    var footer_top = footer[0].getBoundingClientRect().top;
    
    
    if (footer_top - header_top < 50) {
        header.css("background-color", "#e1eaec")
    } else if (header_top - base_grid_top > 12) {
        header.css("background-color", "#ffffff");
    } else {
        header.css("background-color", "#e0edef")
    }
    
});