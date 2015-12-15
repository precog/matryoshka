[![Build status](https://travis-ci.org/quasar-analytics/quasar.svg?branch=master)](https://travis-ci.org/quasar-analytics/quasar)
[![Coverage Status](https://coveralls.io/repos/quasar-analytics/quasar/badge.svg)](https://coveralls.io/r/quasar-analytics/quasar)
[![Join the chat at https://gitter.im/quasar-analytics/quasar](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/quasar-analytics/quasar?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

**Issues for this project are kindly hosted by [Atlassian JIRA](https://slamdata.atlassian.net). Signup is open to anyone, so if you want to contribute, have bugs to report or features to suggest, [sign up for a JIRA account](https://slamdata.atlassian.net).**

# Quasar

Quasar is an open source NoSQL analytics engine that can be used as a library or through a REST API to power advanced analytics across a growing range of data sources and databases, including MongoDB.

**This is the open source site for Quasar, a NoSQL analytics engine. If you are looking for the SlamData application (which is built on Quasar), please visit the [official SlamData website](http://slamdata.com) for pre-built installers.**

## Using the Pre-Built JARs

In [Github Releases](http://github.com/quasar-analytics/quasar/releases), you can find pre-built JARs for all the subprojects in this repository.

See the instructions below for running and configuring these JARs.

## Building from Source

**Note**: This requires Java 8 and Bash (Linux, Mac, or Cygwin on Windows).

### Checkout

```bash
git clone git@github.com:quasar-analytics/quasar.git
```

### Build

The following sections explain how to build and run the various subprojects.

#### Basic Compile & Test

To compile the project and run tests, execute the following command:

```bash
./sbt test
```

This will lead to failures in the integration test project (`it`). The reason for the failures is the fact that there is no configured
"backend" to connect to in order to run the integration tests. Currently Quasar only supports MongoDB so in order to run the integration
tests, you will need to provide a URL to a MongoDB. If you have a hosted MongoDB instance handy, then you can simply point to it, or else
you probably want to install MongoDB locally and point Quasar to that one. Installing MongoDB locally is probably a good idea as it will
allow you to run the integration tests offline as well as make the tests run as fast as possible.

In order to install MongoDB locally you can either use something like Homebrew or simply go to the MongDB website and follow the
instructions that can be found there.

Once we have a MongoDB instance handy, we need to set a few
environment variables in order to inform Quasar about where to find the backends required in order to run the integration tests.

Set the following environment variables:

For bash this would like such,

```bash
export QUASAR_MONGODB_3_2="{\"mongodb\":{\"connectionUri\":\"mongodb://`mongoURL`\"}}"
export QUASAR_MONGODB_3_0="{\"mongodb\":{\"connectionUri\":\"mongodb://`mongoURL`\"}}"
export QUASAR_MONGODB_2_6="{\"mongodb\":{\"connectionUri\":\"mongodb://`mongoURL`\"}}"
```

where \`mongoURL\` is the url at which one can find a Mongo database. For example \`mongoURL\` would probably look
something like `localhost:27017` for a local installation. This means the integration tests will be run against
both MongoDB versions 2.6, 3.0, and 3.2. Alternatively, you can choose to install only one of these and run the integration
tests against only that one database. Simply omit a version in order to avoid testing against it. On the integration
server, the tests are run against all supported versions of MongoDB, as well as read-only configurations.

#### REPL Jar

To build a JAR for the REPL, which allows entering commands at a command-line prompt, execute the following command:

```bash
./sbt 'project core' oneJar
```

The path of the JAR will be `./core/target/scala-2.11/core_2.11-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [<config file>]
```

#### Web JAR

To build a JAR containing a lightweight HTTP server that allows you to programmatically interact with Quasar, execute the following command:

```bash
./sbt 'project web' oneJar
```

The path of the JAR will be `./web/target/scala-2.11/web_2.11-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [-c <config file>]
```

### Configure

The various JARs can be configured by using a command-line argument to indicate the location of a JSON configuration file. If no config file is specified, it is assumed to be `quasar-config.json`, from a standard location in the user's home directory.

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

#### Database mounts

If the mount's key is "mongodb", then the `connectionUri` is a standard [MongoDB connection string](http://docs.mongodb.org/manual/reference/connection-string/). Only the primary host is required to be present, however in most cases a database name should be specified as well. Additional hosts and options may be included as specified in the linked documentation.

For example, say a MongoDB instance is running on the default port on the same machine as Quasar, and contains databases `test` and `students`, the `students` database contains a collection `cs101`, and the configuration looks like this:
```json
  "mountings": {
    "/local/": {
      "mongodb": {
        "connectionUri": "mongodb://localhost/test"
      }
    }
  }
```
Then the filesystem will contain the paths `/local/test/` and `/local/students/cs101`, among others.

A database can be mounted at any directory path, but database mount paths must not be nested inside each other.

#### View mounts

If the mount's key is "view" then the mount represents a "virtual" file, defined by a SQL query. When the file's contents are read or referred to, the query is executed to generate the current result on-demand. A view can be used to create dynamic data that combines analysis and formatting of existing files without creating temporary results that need to be manually regenerated when sources are updated.

For example, given the above MongoDB mount, an additional view could be defined in this way:

```json
  "mountings": {
    ...,
    "/simpleZips": {
      "view": {
        "connectionUri": "sql2:///?q=select%20_id%20as%20zip%2C%20city%2C%20state%20from%20%22%2Flocal%2Ftest%2Fzips%22%20where%20pop%20%3C%20%3Acutoff&var.cutoff=1000"
      }
    }
  }
```

A view can be mounted at any file path. If a view's path is nested inside the path of a database mount, it will appear alongside the other files in the database. A view will "shadow" any actual file that would otherwise be mapped to the same path. Any attempt to write data to a view will result in an error.

SQL<sup>2</sup> supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as additional parameters in the connectionUri using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error when the mount is created or used. These values use the same syntax as the query itself; notably, strings should be surrounded by single quotes. Some acceptable values are `123`, `'CO'`, and `DATE '2015-07-06'`.

## REPL Usage

The interactive REPL accepts SQL `SELECT` queries.

First, choose the database to be used. Here, a MongoDB instance is mounted at
the root, and it contains a database called `test`:

```
💪 $ cd test
```

The "tables" in SQL queries refer to collections in the database by name:
```
💪 $ select * from zips where state='CO' limit 3
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

💪 $ select city from zips limit 3
...
 city     |
----------|
 AGAWAM   |
 CUSHMAN  |
 BARRE    |
```

You may also store the result of a SQL query:

```sql
💪 $ out1 := select * from zips where state='CO' limit 3
```

The location of a collection may be specified as an absolute path by
surrounding the path with double quotes:

```sql
select * from "/test/zips"
```

Type `help` for information on other commands.


## API Usage

The server provides a simple JSON API.

### GET /query/fs/[path]?q=[query]&offset=[offset]&limit=[limit]&var.[foo]=[value]

Executes a SQL query, contained in the required `q` parameter, on the backend responsible for the request path.

Optional `offset` and `limit` parameters can be specified to page through the results, and are interpreted the same way as in `GET /data`.

SQL<sup>2</sup> supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as query parameters in this API using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by single quotes. Some acceptable values are `123`, `'CO'`, and `DATE '2015-07-06'`.

The result is returned in the response body. An `Accept` header may be specified to select the format of the response body:
- no `Accept` header: “readable” results, one per line (note: this response cannot be parsed as a single JSON object)
- `Accept: application/json`: “readable” results wrapped in a single JSON array
- `Accept: application/ldjson;mode=precise`: “precise” JSON, one result per line
- `Accept: text/csv`: comma-separated

The formatting of CSV output can be controlled with an extended media type as in `Accept: text/csv; columnDelimiter="|"&rowDelimiter=";"&quoteChar="'"&escapeChar="\"`.

For compressed output use `Accept-Encoding: gzip`.


### POST /query/fs/[path]?var.[foo]=[value]

Executes a SQL query, contained in the request body, on the backend responsible for the request path.

The `Destination` header must specify the *output path*, where the results of the query will become available if this API successfully completes.

All paths referenced in the query, as well as the output path, are interpreted as relative to the request path, unless they begin with `/`.

SQL<sup>2</sup> supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as query parameters in this API using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by single quotes. Some acceptable values are `123`, `'CO'`, and `DATE '2015-07-06'`.

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

### GET /compile/fs/[path]?q=[query]&var.[foo]=[value]

Compiles, but does not execute, a SQL query, contained in the single, required
query parameter, on the backend responsible for the request path. The resulting
plan is returned in the response body.

SQL<sup>2</sup> supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as query parameters in this API using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by single quotes. Some acceptable values are `123`, `'CO'`, and `DATE '2015-07-06'`.


### POST /compile/fs/[path]?var.[foo]=[value]

Compiles, but does not execute, a SQL query, contained in the request body.
The resulting plan is returned in the response body.

SQL<sup>2</sup> supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as query parameters in this API using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by single quotes. Some acceptable values are `123`, `'CO'`, and `DATE '2015-07-06'`.


### GET /metadata/fs/[path]

Retrieves metadata about the files, directories, and mounts which are children of the specified directory path. If the path names a file, the result is empty.

```json
{
  "children": [
    {"name": "foo", "type": "directory"},
    {"name": "bar", "type": "file"},
    {"name": "test", "type": "directory", "mount": "mongodb"},
    {"name": "baz", "type": "file", "mount": "view"}
  ]
}
```

### GET /data/fs/[path]?offset=[offset]&limit=[limit]

Retrieves data from the specified path, formatted in JSON or CSV format. The `offset` and `limit` parameters are optional, and may be used to page through results.

```json
{"id":0,"guid":"03929dcb-80f6-44f3-a64c-09fc1d810c61","isActive":true,"balance":"$3,244.51","picture":"http://placehold.it/32x32","age":38,"eyeColor":"green","latitude":87.709281,"longitude":-20.549375}
{"id":1,"guid":"09639710-7f99-4fe1-a890-b1b592cbe223","isActive":false,"balance":"$1,544.65","picture":"http://placehold.it/32x32","age":27,"eyeColor":"blue","latitude":52.394181,"longitude":-0.631589}
{"id":2,"guid":"e71b7f01-ce0e-4824-ad1e-4e118872aec4","isActive":true,"balance":"$1,882.92","picture":"http://placehold.it/32x32","age":24,"eyeColor":"green","latitude":30.061766,"longitude":-106.813523}
{"id":3,"guid":"79602676-6f63-41d0-9c0a-a4f5851a43db","isActive":false,"balance":"$1,281.00","picture":"http://placehold.it/32x32","age":25,"eyeColor":"blue","latitude":14.713939,"longitude":62.253264}
{"id":4,"guid":"0024a8ad-373f-459a-8316-d50d7a8f7b10","isActive":true,"balance":"$1,908.50","picture":"http://placehold.it/32x32","age":26,"eyeColor":"brown","latitude":-21.874648,"longitude":67.270659}
{"id":5,"guid":"f7e33b92-a885-450e-8ad5-92103b1f5ff3","isActive":true,"balance":"$2,231.90","picture":"http://placehold.it/32x32","age":31,"eyeColor":"blue","latitude":58.461107,"longitude":176.40584}
{"id":6,"guid":"a2863ec1-9652-46d3-aa12-aa92308de055","isActive":false,"balance":"$1,621.67","picture":"http://placehold.it/32x32","age":34,"eyeColor":"blue","latitude":-83.908456,"longitude":67.190633}
```

The output format can be selected using an `Accept` header as described above.

Given a directory path (ending with a slash), produces a `zip` archive containing the contents of the named directory, database, etc. Each file in the archive is formatted as specified in the request query and/or `Accept` header.

### PUT /data/fs/[path]

Replaces data at the specified path, formatted as one JSON object per line in the same format as above.
Either succeeds, replacing any previous contents atomically, or else fails leaving the previous contents
unchanged.

If an error occurs when reading data from the request body, the response contains a summary in the common `error` field, and a separate array of error messages about specific values under `details`.

Fails if the path identifies a view.

### POST /data/fs/[path]

Appends data to the specified path, formatted as one JSON object per line in the same format as above.
If an error occurs, some data may have been written, and the content of the response describes what
was done.

If an error occurs when reading data from the request body, the response contains a summary in the common `error` field, and a separate array of error messages about specific values under `details`.

Fails if the path identifies a view.

### DELETE /data/fs/[path]

Removes all data at the specified path. Single files are deleted atomically.

Fails if the path identifies a view (views may be added and deleted through the `/mount` API).

### MOVE /data/fs/[path]

Moves data from one path to another within the same backend. The new path must
be provided in the "Destination" request header. Single files are moved atomically.

Fails if either the request of destination path identifies a view (views may be added and deleted through the `/mount` API).

### GET /mount/fs/[path]

Retrieves the configuration for the mount point at the provided path. In the case of MongoDB, the response will look like

```
{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }
```

The outer key is the backend in use, and the value is a backend-specific configuration structure.

### POST /mount/fs/[path]

Adds a new mount point using the JSON contained in the body. The path is the containing directory, and an `X-File-Name` header should contain the name of the mount. This will return a 409 Conflict if the mount point already exists or if a database mount already exists above or below a new database mount.

### PUT /mount/fs/[path]

Creates a new mount point or replaces an existing mount point using the JSON contained in the body. This will return a 409 Conflict if a database mount already exists above or below a new database mount.

### DELETE /mount/fs/[path]

Deletes an existing mount point, if any exists at the given path. If no such mount exists, the request succeeds but the response has no content.

### PUT /server/port

Takes a port number in the body, and restarts the server on that port, shutting down the running instance.

### DELETE /server/port

Removes any configured port, reverting to the default (20223) and restarting, as with `PUT`.


## Request Headers

Request headers may be supplied via a query parameter in case the client is unable to send arbitrary headers (e.g. browsers, in certain circumstances). The parameter name is `request-headers` and the value should be a JSON-formatted string containing an object whose fields are named for the corresponding header and whose values are strings or arrays of strings. If any header appears both in the `request-headers` query parameter and also as an ordinary header, the query parameter takes precedence.

For example:
```
GET http://localhost:8080/data/fs/local/test/foo?request-headers=%7B%22Accept%22%3A+%22text%2Fcsv%22%7D
```
Note: that's the URL-encoded form of `{"Accept": "text/csv"}`.

## Data Formats

Quasar produces and accepts data in two JSON-based formats or CSV. Each JSON-based format can
represent all the types of data that Quasar supports. The two formats are appropriate for
different purposes.

### Precise JSON

This format is unambiguous, allowing every value of every type to be specified. It's useful for
entering data, and for extracting data to be read by software (as opposed to people.) Contains
extra information that can make it harder to read.


### Readable JSON

This format is easy to read and use with other tools, and contains minimal extra information.
It does not always convey the precise type of the source data, and does not allow all values
to be specified. For example, it's not possible to tell the difference between the string
`"12:34:56"` and the time value equal to 34 minutes and 56 seconds after noon.


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


### CSV

When Quasar produces CSV, all fields and array elements are "flattened" so that each column in the output contains the data for a single location in the source document. For example, the document `{ "foo": { "bar": 1, "baz": 2 } }` becomes

```
foo.bar,foo.baz
1,2
```

Data is formatted the same way as the "Readable" JSON format, except that all values including `null`, `true`, `false`, and numbers are indistinguishable from their string representations.

When data is uploaded in CSV format, the headers are interpreted as field names in the same way. As with the Readable JSON format, any string that can be interpreted as another kind of value will be, so for example there's no way to specify the string `"null"`.


## Troubleshooting

First, make sure that the `quasar-analytics/quasar` Github repo is building correctly (the status is displayed at the top of the README).

Then, you can try the following command:

```bash
./sbt test
```

This will ensure that your local version is also passing the tests.

Check to see if the problem you are having is mentioned in the [JIRA issues](https://slamdata.atlassian.net/) and, if it isn't, feel free to create a new issue.

You can also discuss issues on Gitter: [quasar-analytics/quasar](https://gitter.im/quasar-analytics/quasar).

## Legal

Copyright &copy; 2014 - 2015 SlamData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
