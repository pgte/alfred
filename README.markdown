Alfred is a fast in-process key-value store for node.js.

Alfred supports:

* multiple key-value maps in one database
* atomic operations on one record
* finder streams
* activity streams
* append-only files
* compactation
* buffered and unbuffered writes
* in-memory indexes
* sorting
* replication
* integrity check

[More...](http://pgte.github.com/alfred/features.html)

Install via npm:

    $ npm install alfred

Example:

    var Alfred = require('alfred');
    // Open database
    Alfred.open('path/to/db', function(err, db) {
      if (err) { throw err; }
 
      // define User model and its properties
      var User = db.define('User', {
        indexes: [{name: 'age',
                  fn: function(user) { return user.age; }]
      });
      User.property('name', 'string', {
        maxLength: 100
      });
      User.property('active', 'boolean');
 
      // get user by id
      User.get(id, function(user) {
        console.log(user.inspect());
      };
 
      // find users
      User.find({age: {$gt: 18}}).all(function(users) {
        console.log('Found ' + users.length + ' users with more than 18 years') ;
      });
    });
    
[Read more about it](http://pgte.github.com/alfred)
