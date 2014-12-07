#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ "$TRAVIS_BUILD" = "true" ] ; then
  "$DIR/../sbt" coverageAggregate coveralls
else
  "$DIR/../sbt" coverageAggregate
fi
