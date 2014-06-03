# SlamEngine

The NoSQL analytics engine that powers SlamData.

This is the open source site for SlamData for people who want to hack on or contribute to the development of SlamData.

**For pre-built installers for the SlamData application, please visit the [official SlamData website](http://slamdata.com).**

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

## Run the REPL

```bash
java -cp slamengine.jar slamdata.engine.repl.Repl [config file]
```

# Legal

Released under the GNU AFFERO GENERAL PUBLIC LICENSE. See `LICENSE` file in the repository.

Copyright 2014 SlamData Inc.
