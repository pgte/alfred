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
app.get('/api/odm', function(req, res) {
  res.render('api/odm');
});
app.get('/api/database', function(req, res) {
  res.render('api/database');
});
app.get('/api/key_map', function(req, res) {
  res.render('api/key_map');
});
app.get('/api/find', function(req, res) {
  res.render('api/find');
});
app.get('/api/find/operators', function(req, res) {
  res.render('api/find_operators');
});
app.get('/api/replication', function(req, res) {
  res.render('api/replication');
});

app.get('/examples', function(req, res) {
  res.render('examples/index');
});
app.get('/examples/odm', function(req, res) {
  res.render('examples/odm');
});
app.get('/examples/raw', function(req, res) {
  res.render('examples/raw');
});

app.get('/features', function(req, res) {
  res.render('features/index');
});

app.get('/internals', function(req, res) {
  res.render('internals/index');
});
app.get('/internals/model', function(req, res) {
  res.render('internals/model');
});
app.get('/internals/files', function(req, res) {
  res.render('internals/files');
});
app.get('/internals/replication', function(req, res) {
  res.render('internals/replication');
});

app.get('/benchmarks', function(req, res) {
  res.render('benchmarks/index');
});

app.listen(4000);