var jsdom  = require("jsdom"),
    window = jsdom.jsdom().createWindow();
    
jsdom.jQueryify(window, __dirname + '/../dist/jquery.min.js' , function() {
  window.$('body').append('<div class="testing">Hello World, It works</div>');
  console.log(window.$('.testing').text());
  var flot_script = window.document.createElement("script");
  flot_script.src = 'file://' + __dirname + '/../dist/flot/jquery.flot.js';
  window.document.head.appendChild(flot_script);
  flot_script.onload = function() {
    window.$('body').append('<div id="placeholder"></div>');

    var d1 = [];
    for (var i = 0; i < 14; i += 0.5)
        d1.push([i, Math.sin(i)]);

    var d2 = [[0, 3], [4, 8], [8, 5], [9, 13]];

    // a null signifies separate line segments
    var d3 = [[0, 12], [7, 12], null, [7, 2.5], [12, 2.5]];

    window.$.plot(window.$("#placeholder"), [ d1, d2, d3 ]);
  }
  
});