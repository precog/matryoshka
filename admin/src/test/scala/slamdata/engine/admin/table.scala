package slamdata.engine.admin

import org.specs2.mutable._

import scala.collection.immutable.ListMap

import argonaut._
import Argonaut._

class TableSpecs extends Specification {
  "Values.flatten" should {
    "find single field" in {
      val json = Json("a" := 1)
      Values.flatten(json) must_== ListMap(List("a") -> "1")
    }

    "find multiple fields" in {
      val json = Json("a" := jNull, "b" := true, "c" := false, "d" := 1.0, "e" := "foo")
      Values.flatten(json) must_== ListMap(
        List("a") -> "",
        List("b") -> "true",
        List("c") -> "false",
        List("d") -> "1",
        List("e") -> "foo")
    }

    "find nested fields" in {
      val json = Json("value" := Json("a" := 1, "b" := 2))
      Values.flatten(json) must_== ListMap(
        List("value", "a") -> "1",
        List("value", "b") -> "2")
    }

    "find array indices" in {
      val json = Json("arr" := List("a", "b"))
      Values.flatten(json) must_== ListMap(
        List("arr", "0") -> "a",
        List("arr", "1") -> "b")
    }

    "find nested array indices" in {
      val json = Json("arr" := List(Json("a" := "foo"), Json("a" := "bar")))
      Values.flatten(json) must_== ListMap(
        List("arr", "0", "a") -> "foo",
        List("arr", "1", "a") -> "bar")
    }
    
    "handle encoded date" in {
      val json = Json("date" := Json("$date" := "2014-08-17T06:00:00.000Z"))
      Values.flatten(json) must_== ListMap(List("date") -> "2014-08-17T06:00:00.000Z")
    }

    "handle encoded id" in {
      val json = Json("_id" := Json("$oid" := "549324efc2b42971b4217930"))
      Values.flatten(json) must_== ListMap(List("_id") -> "549324efc2b42971b4217930")
    }
  }
}