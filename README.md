# SlamEngine

The NoSQL analytics engine that powers SlamData.

This is the open source site for SlamData for people who want to hack on or contribute to the development of SlamData.

[![Build status](https://travis-ci.org/slamdata/slamengine.svg?branch=master)](https://travis-ci.org/slamdata/slamengine)

**For pre-built installers for the SlamData application, please visit the [official SlamData website](http://slamdata.com).**

## Building from Source

**Note**: This requires Java 7.

### Checkout

```bash
git clone git@github.com:slamdata/slamengine.git
```

### Build

```bash
./sbt one-jar
```

The build process will ask
```
Multiple main classes detected, select one to run:

 [1] slamdata.engine.repl.Repl
 [2] slamdata.engine.api.Server
```
Choose `1` for a jar that offers a standalone interactive session, or `2` for a server that the SlamData front-end (or any HTTP client) can talk to.

The path of the JAR will be `./target/scala-2.10/slamengine_2.10-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the SlamEngine version number.

### Configure

Create a configuration file with the following format:

```json
{
  "server": {
    "port": 8080
  },

  "mountings": {
    "/": {
      "mongodb": {
        "database":       "foo",
        "connectionUri":  "mongodb://..."
      }
    }
  }
}
```

### Run

```bash
java -jar path/to/slamengine.jar [config file]
```

## REPL Usage

The interactive REPL accepts SQL `SELECT` queries.

```
slamdata$ select * from zips where state='CO' limit 3
...
{ "_id" : "80002" , "city" : "ARVADA" , "loc" : [ -105.098402 , 39.794533] , "pop" : 12065 , "state" : "CO"}
{ "_id" : "80003" , "city" : "ARVADA" , "loc" : [ -105.065549 , 39.828572] , "pop" : 32980 , "state" : "CO"}
{ "_id" : "80004" , "city" : "ARVADA" , "loc" : [ -105.11771 , 39.814066] , "pop" : 33260 , "state" : "CO"}

slamdata$ select city from zips limit 3
...
{ "_id" : "35004" , "city" : "ACMAR"}
{ "_id" : "35005" , "city" : "ADAMSVILLE"}
{ "_id" : "35006" , "city" : "ADGER"}
```

## API Usage

The server launches a simple JSON API.

### POST /query/fs/[path]?out=tmp231

Executes the specified SQL query at the specified path. Returns the name where the results are stored.

```json
{
  "out": "/[path]/tmp231",
  "phases": [
    ...
  ]
}
```

If an error occurs while compiling or executing the query, a 500 response is 
produced, with this content:

```json
{
  "error": "[very large error text]",
  "phases": [
    ...
  ]
}
```

The "phases" array contains a sequence of objects containing the result from
each phase of the query compilation process. A phase may result in a tree of 
objects with "label" and (optional) "children":

```json
{
  ...,
  "phases": [
    {
      "name": "SQL AST",
      "tree": {
        "label": "SelectStmt",
        "children": [
          ...
        ]
      }
    },
    ...
  ]
}
```

Or a blob of text:

```json
{
  ...,
  "phases": [
    ...,
    {
      "name": "Mongo",
      "detail": "db.zips.aggregate([\n  { \"$sort\" : { \"pop\" : 1}}\n])\n"
    }
  ]
}
```

Or an error (typically no further phases appear, and the error repeats the 
error at the root of the response):

```json
{
  ...,
  "phases": [
    ...,
    {
      "name": "Physical Plan",
      "error": "Cannot compile ..."
    }
  ]
}
```

### GET /metadata/fs/[path]

Retrieves metadata about the specified path.

```json
{
  "children": [{"name": ".", "type": "directory"}, {"name": "bar", "type": "file"}]
}
```

### GET /data/fs/[path]?offset=[offset]&limit=[limit]

Retrieves data from the specified path, formatted as one JSON object per line. The `offset` and `limit` parameters are optional, and may be used to page through results.

```json
{"id":0,"guid":"03929dcb-80f6-44f3-a64c-09fc1d810c61","isActive":true,"balance":"$3,244.51","picture":"http://placehold.it/32x32","age":38,"eyeColor":"green","latitude":87.709281,"longitude":-20.549375}
{"id":1,"guid":"09639710-7f99-4fe1-a890-b1b592cbe223","isActive":false,"balance":"$1,544.65","picture":"http://placehold.it/32x32","age":27,"eyeColor":"blue","latitude":52.394181,"longitude":-0.631589}
{"id":2,"guid":"e71b7f01-ce0e-4824-ad1e-4e118872aec4","isActive":true,"balance":"$1,882.92","picture":"http://placehold.it/32x32","age":24,"eyeColor":"green","latitude":30.061766,"longitude":-106.813523}
{"id":3,"guid":"79602676-6f63-41d0-9c0a-a4f5851a43db","isActive":false,"balance":"$1,281.00","picture":"http://placehold.it/32x32","age":25,"eyeColor":"blue","latitude":14.713939,"longitude":62.253264}
{"id":4,"guid":"0024a8ad-373f-459a-8316-d50d7a8f7b10","isActive":true,"balance":"$1,908.50","picture":"http://placehold.it/32x32","age":26,"eyeColor":"brown","latitude":-21.874648,"longitude":67.270659}
{"id":5,"guid":"f7e33b92-a885-450e-8ad5-92103b1f5ff3","isActive":true,"balance":"$2,231.90","picture":"http://placehold.it/32x32","age":31,"eyeColor":"blue","latitude":58.461107,"longitude":176.40584}
{"id":6,"guid":"a2863ec1-9652-46d3-aa12-aa92308de055","isActive":false,"balance":"$1,621.67","picture":"http://placehold.it/32x32","age":34,"eyeColor":"blue","latitude":-83.908456,"longitude":67.190633}
```

## Troubleshooting

First, make sure that the slamdata/slamengine Github repo is building correctly (the status is displayed at the top of the README). Then, you can use
```bash
./sbt test
```
to ensure that your local version is also passing the tests.

Check to see if the problem you are having is mentioned in the [Github issues](https://github.com/slamdata/slamengine/issues) and, if it isn’t, feel free to create a new issue.

You can also discuss issues on the SlamData IRC channel: [#slamdata](irc://chat.freenode.net/%23slamdata) on [Freenode](http://freenode.net).

## Legal

Released under the GNU AFFERO GENERAL PUBLIC LICENSE. See `LICENSE` file in the repository.

Copyright 2014 SlamData Inc.
