var express = require('express');

var app = express.createServer();

app.configure(function() {
  app.use(express.logger());
  app.use(express.bodyDecoder());
  app.use(express.staticProvider(__dirname + '/static'));
});

app.configure('development', function () {
  app.use(express.errorHandler({
    dumpExceptions: true,
    showStack: true
  }));
});

app.configure('production', function () {
  app.use(express.errorHandler());
});

app.set('views', __dirname + '/views');
app.set('view engine', 'jade');

app.get('/', function(req, res) {
  res.render('content/index');
});

app.get('/api', function(req, res) {
  res.render('api/index');
});
app.get('/api/database', function(req, res) {
  res.render('api/database');
});
app.get('/api/key_map', function(req, res) {
  res.render('api/key_map');
});

app.get('/benchmarks', function(req, res) {
  res.render('benchmarks/index');
});

app.listen(4000);