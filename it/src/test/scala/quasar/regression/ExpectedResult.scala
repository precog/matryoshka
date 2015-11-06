package quasar
package regression

import quasar.Predef._

import argonaut._, Json._

case class ExpectedResult(
  rows:           List[Json],
  predicate:      Predicate,
  ignoredFields:  List[JsonField]
)
