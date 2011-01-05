#!/bin/bash
rm -rf localhost:4000
rm -rf api api.html benchmarks.html examples.html features.html index.html javascripts stylesheets summaries
wget --mirror –w 2 –p -E -P . --convert-links http://localhost:4000
wget --mirror –w 2 –p -E -P . --convert-links http://localhost:4000/summaries/latest
mv localhost:4000/* .
rmdir localhost:4000
