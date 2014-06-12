# SlamEngine

The NoSQL analytics engine that powers SlamData.

This is the open source site for SlamData for people who want to hack on or contribute to the development of SlamData.

### For pre-built installers for the SlamData application, please visit the [official SlamData website](http://slamdata.com).

## Checkout

```bash
git clone git@github.com:slamdata/slamengine.git .
```

## Build

```bash
./sbt
test
```

## Package

```bash
./sbt
one-jar
```

The path of the JAR will be `./target/scala-2.10/slamengine_2.10-[version]-SNAPSHOT-one-jar.jar`, where `[version]` is the version number.

## Configure

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

## API

The server launches a simple JSON API.

### POST /query/fs/[path]?out=tmp231

Executes the specified query at the specified path. Returns the name where the results are stored.

```json
{
  "out": "/[path]/tmp231"
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

## Run the Server

```bash
java -cp slamengine.jar slamdata.engine.api.Server [config file]
```

## Run the REPL

```bash
java -cp slamengine.jar slamdata.engine.repl.Repl [config file]
```

# Legal

Released under the GNU AFFERO GENERAL PUBLIC LICENSE. See `LICENSE` file in the repository.

Copyright 2014 SlamData Inc.
