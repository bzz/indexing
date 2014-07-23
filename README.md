Inverted Index on MapReduce
===========================

This is an example of building inverted index on MapReduce.
It uses:
 - more compact on disk index representation then naiv "text" from [YDN example](https://developer.yahoo.com/hadoop/tutorial/module4.html)
 - Lucene analyzer to Tokenyze\filter stopwords
 
 
TODO:
 - [ ] measure perf
 - [ ] document memody constraints\assumptions
 - [ ] optimize: reuse writables
 - [ ] optimize: varible lenght encoding for digits
 - [ ] add unitTests (MRUnit)
 - [ ] switch to MRv2 api