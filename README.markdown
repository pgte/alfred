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
      // find and log all users with age > 25 and <= 40
      db.users.find({age: {$gt : 25, $lte: 40}}) (function(err, user) {
        if (err) { throw err; }
        console.log(user);
      });
    });

[Read more about it](http://pgte.github.com/alfred)
