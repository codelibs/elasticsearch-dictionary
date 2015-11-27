Elasticsearch Dictionary
=======================

## Overview

Dictionary Plugin provides a feature to manage dictionary files for indices.
Installing this plugin, elasticsearch is able to create/restore a snapshot with dictionary files automatically.

## Version

| Version   | Tested on Elasticsearch |
|:---------:|:-----------------------:|
| master    | 1.7.X                   |
| 1.7.0     | 1.7.1                   |
| 1.5.0     | 1.5.0                   |
| 1.4.0     | 1.4.1                   |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-dictionary/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install Dictionary Plugin

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-dictionary/1.7.0

## References

### Snapshot/Restore With Dictionary

A default Elasticsearch's Snapshot/Restore does not contain dictionary files.
However, if the dictionary files does not exist, Elasticsearch does not work when starting or opening an index.

In this plugin, an extended Snapshot/Restore feature is provided.
This plugin finds dictionary files in an index setting and then creates an additional snapshot for storing the dictionary files automatically.
Moreover, for restoring the snapshot, the dictionary files are restored when restoring the target snapshot.
If installing this plugin, the above will work automatically.



