[![Build status](https://travis-ci.org/slamdata/slamengine.svg?branch=master)](https://travis-ci.org/slamdata/slamengine)
[![Coverage Status](https://img.shields.io/coveralls/slamdata/slamengine.svg)](https://coveralls.io/r/slamdata/slamengine)
[![Stories in Ready](https://badge.waffle.io/slamdata/slamengine.png?label=ready&title=Ready)](https://waffle.io/slamdata/slamengine)

# SlamEngine

The NoSQL analytics engine that powers SlamData.

This is the open source site for SlamData for people who want to hack on or contribute to the development of SlamEngine.

**For pre-built installers for the SlamData application, please visit the [official SlamData website](http://slamdata.com).**

**Note**: SlamData only supports MongoDB 2.6.X and higher.

## Using the Pre-Built JARs

In [Github Releases](http://github.com/slamdata/slamengine/releases), you can find pre-built JARs for all the subprojects in this repository.

See the instructions below for running and configuring these JARs.

## Building from Source

**Note**: This requires Java 7 and Bash (Linux, Mac, or Cygwin on Windows).

### Checkout

```bash
git clone git@github.com:slamdata/slamengine.git
```

### Build

The following sections explain how to build and run the various subprojects.

#### Basic Compile & Test

To compile the project and run tests, execute the following command:

```bash
./sbt test
```

#### REPL Jar

To build a JAR for the REPL, which allows entering commands at a command-line prompt, execute the following command:

```bash
./sbt 'project core' oneJar
```

The path of the JAR will be `./core/target/scala-2.11/core_2.11-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the SlamEngine version number.

To run the JAR, execute the following command:

```bash
java -jar [path to jar] [config file]
```

#### Web JAR

To build a JAR containing a lightweight HTTP server that allows you to programmatically interact with SlamEngine, execute the following command:

```bash
./sbt 'project web' oneJar
```

The path of the JAR will be `./web/target/scala-2.11/web_2.11-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the SlamEngine version number.

To run the JAR, execute the following command:

```bash
java -jar [path to jar] [config file]
```

#### Admin JAR

To build a JAR containing a GUI admin tool that can be used for testing connections to MongoDB, execute the following command:

```bash
./sbt 'project admin' oneJar
```

The path of the JAR will be `./admin/target/scala-2.11/admin_2.11-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the SlamEngine version number.

To run the JAR, execute the following command:

```bash
java -jar [path to jar] [config file]
```

### Configure

The various JARs can be configured by using a command-line argument to indicate the location of a JSON configuration file. If no config file is specified, it is assumed to be `slamengine-config.json`, from a standard location in the user's home directory.

The JSON configuration file must have the following format:

```json
{
  "server": {
    "port": 8080
  },

  "mountings": {
    "/": {
      "mongodb": {
        "connectionUri": "mongodb://<user>:<pass>@<host>:<port>/<dbname>"
      }
    }
  }
}
```

One or more mountings may be included, and each must have a unique path (above, `/`), which determines where in the filesystem the database(s) contained by the mounting will appear.

The `connectionUri` is a standard [MongoDB connection string](http://docs.mongodb.org/manual/reference/connection-string/). Only the primary host is required to be present, however in most cases a database name should be specified as well. Additional hosts and options may be included as specified in the linked documentation.

For example, say a MongoDB instance is running on the default port on the same machine as SlamData, and contains databases `test` and `students`, the `students` database contains a collection `cs101`, and the configuration looks like this:
```json
  "mountings": {
    "/local": {
      "mongodb": {
        "connectionUri": "mongodb://localhost/test"
      }
    }
  }
```
Then the filesystem will contain the paths `/local/test/` and `/local/students/cs101`, among others.


## REPL Usage

The interactive REPL accepts SQL `SELECT` queries.

First, choose the database to be used. Here, a MongoDB instance is mounted at
the root, and it contains a database called `test`:

```
slamdata$ cd test
```

The "tables" in SQL queries refer to collections in the database by name:
```
slamdata$ select * from zips where state='CO' limit 3
Mongo
db.zips.aggregate(
  [
    { "$match": { "state": "CO" } },
    { "$limit": NumberLong(3) },
    { "$out": "tmp.gen_0" }],
  { "allowDiskUse": true });
db.tmp.gen_0.find();


Query time: 0.1s
 city    | loc[0]       | loc[1]     | pop    | state |
---------|--------------|------------|--------|-------|
 ARVADA  |  -105.098402 |  39.794533 |  12065 | CO    |
 ARVADA  |  -105.065549 |  39.828572 |  32980 | CO    |
 ARVADA  |   -105.11771 |  39.814066 |  33260 | CO    |

slamdata$ select city from zips limit 3
...
 city     |
----------|
 AGAWAM   |
 CUSHMAN  |
 BARRE    |
```

You may also store the result of a SQL query:

```sql
slamdata$ out1 := select * from zips where state='CO' limit 3
```

The location of a collection may be specified as an absolute path by
surrounding the path with double quotes:

```sql
select * from "/test/zips"
```

Type `help` for information on other commands.


## API Usage

The server provides a simple JSON API.


### GET /query/fs/[path]?q=[query]

Executes a SQL query, contained in the single, required query parameter, on the backend responsible for the request path.

The result is returned in the response body, formatted as one JSON object per line. By default, the “readable” JSON format (described below) is returned, but the “precise” format can be requested by including a header like `Accept: application/ldjson;mode=precise`.

SQL `limit` syntax may be used to keep the result size reasonable.


### POST /query/fs/[path]?foo=var

Executes a SQL query, contained in the request body, on the backend responsible for the request path.

The `Destination` header must specify the *output path*, where the results of the query will become available if this API successfully completes.

All paths referenced in the query, as well as the output path, are interpreted as relative to the request path, unless they begin with `/`.

SlamSQL supports variables inside queries (`SELECT * WHERE pop > :cutoff`). Values for these variables should be specified as query parameters in this API. Failure to specify valid values for all variables used inside a query will result in an error.

This API method returns the name where the results are stored, as an absolute path, as well as logging information.

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

The `phases` array contains a sequence of objects containing the result from
each phase of the query compilation process. A phase may result in a tree of
objects with `type`, `label` and (optional) `children`:

```json
{
  ...,
  "phases": [
    ...,
    {
      "name": "Logical Plan",
      "tree": {
        "type": "LogicalPlan/Let",
        "label": "'tmp0",
        "children": [
          {
            "type": "LogicalPlan/Read",
            "label": "./zips"
          },
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

### GET /compile/fs/[path]?q=[query]

Compiles, but does not execute, a SQL query, contained in the single, required
query parameter, on the backend responsible for the request path. The resulting
plan is returned in the response body.


### POST /compile/fs/[path]?foo=var

Compiles, but does not execute, a SQL query, contained in the request body.
The resulting plan is returned in the response body.

SlamSQL supports variables inside queries (`SELECT * WHERE pop > :cutoff`). Values
for these variables should be specified as query parameters in this API. Failure
to specify valid values for all variables used inside a query will result in an error.


### GET /metadata/fs/[path]

Retrieves metadata about the files, directories, and mounts at the specified path.

```json
{
  "children": [
    {"name": "test", "type": "mount"},
    {"name": "foo", "type": "directory"},
    {"name": "bar", "type": "file"}
  ]
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

### PUT /data/fs/[path]

Replaces data at the specified path, formatted as one JSON object per line in the same format as above.
Either succeeds, replacing any previous contents atomically, or else fails leaving the previous contents
unchanged.

### POST /data/fs/[path]

Appends data to the specified path, formatted as one JSON object per line in the same format as above.
If an error occurs, some data may have been written, and the content of the response describes what
was done.

### DELETE /data/fs/[path]

Removes all data at the specified path. Single files are deleted atomically.


### MOVE /data/fs/[path]

Moves data from one path to another within the same backend. The new path must
be provided in the "Destination" request header. Single files are deleted atomically.


## Data Formats

SlamEngine produces and accepts data in two JSON-based formats. Each format is valid JSON, and can
represent all the types of data that SlamEngine supports. The two formats are appropriate for
different purposes.

### Precise JSON

This format is unambiguous, allowing every value of every type to be specified. It's useful for
entering data, and for extracting data to be read by software (as opposed to people.) Contains
extra information that can make it harder to read.


### Readable JSON

This format is easy to read and use with other tools, and contains minimal extra information.
It does not always convey the precise type of the source data, and does not allow all values
to be specified. For example, it's not possible to tell the difference between the string
`"12:34"` and the time value equal to 34 minutes after noon.


### Examples

Type      | Readable        | Precise  | Notes
----------|-----------------|----------|------
null      | `null`          | *same*   |
boolean   | `true`, `false` | *same*   |
string    | `"abc"`         | *same*   |
int       | `1`             | *same*   |
decimal   | `2.1`           | *same*   |
object    | `{ "a": 1 }`    | *same*   |
object    | `{ "$foo": 2 }` | `{ "$obj": { "$foo": 2 } }` | Requires a type-specifier if any key starts with `$`.
array     | `[1, 2, 3]`     | *same*   |
set       | `[1, 2, 3]`     | `{ "$set": [1, 2, 3] }` |
timestamp | `"2015-01-31T10:30:00Z"` | `{ "$timestamp": "2015-01-31T10:30:00Z" }` |
date      | `"2015-01-31"`  | `{ "$date": "2015-01-31" }` |
time      | `"10:30:05"`    | `{ "$time": "10:30:05" }` | HH:MM[:SS[:.SSS]]
interval  | `"PT12H34M"`    | `{ "$interval": "P7DT12H34M" }` | Note: year/month not currently supported.
binary    | `"TE1OTw=="`    | `{ "$binary": "TE1OTw==" }` | BASE64-encoded.
object id | `"abc"`         | `{ "$oid": "abc" }` |


## Troubleshooting

First, make sure that the `slamdata/slamengine` Github repo is building correctly (the status is displayed at the top of the README).

Then, you can try the following command:

```bash
./sbt test
```

This will ensure that your local version is also passing the tests.

Check to see if the problem you are having is mentioned in the [Github issues](https://github.com/slamdata/slamengine/issues) and, if it isn't, feel free to create a new issue.

You can also discuss issues on the SlamData IRC channel: [#slamdata](irc://chat.freenode.net/%23slamdata) on [Freenode](http://freenode.net).

## Legal

Released under the GNU AFFERO GENERAL PUBLIC LICENSE. See `LICENSE` file in the repository.

Copyright &copy; 2014 - 2015 SlamData Inc.
