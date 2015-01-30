package slamdata.engine.admin

import org.specs2.mutable._

import scala.collection.immutable.ListMap

import argonaut._
import Argonaut._

import org.threeten.bp._

import slamdata.engine._

class TableSpecs extends Specification {
  "Values.flatten" should {
    "find single field" in {
      val data = Data.Obj(ListMap("a" -> Data.Int(1)))
      Values.flatten(data) must_== ListMap(List("a") -> Data.Int(1))
    }

    "find multiple fields" in {
      val data = Data.Obj(ListMap(
        "a" -> Data.Null, "b" -> Data.True, "c" -> Data.False, "d" -> Data.Dec(1.0), "e" -> Data.Str("foo")))
      Values.flatten(data) must_== ListMap(
        List("a") -> Data.Null,
        List("b") -> Data.True,
        List("c") -> Data.False,
        List("d") -> Data.Dec(1.0),
        List("e") -> Data.Str("foo"))
    }

    "find nested fields" in {
      val data = Data.Obj(ListMap("value" -> Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2)))))
      Values.flatten(data) must_== ListMap(
        List("value", "a") -> Data.Int(1),
        List("value", "b") -> Data.Int(2))
    }

    "find array indices" in {
      val data = Data.Obj(ListMap("arr" -> Data.Arr(List(Data.Str("a"), Data.Str("b")))))
      Values.flatten(data) must_== ListMap(
        List("arr", "0") -> Data.Str("a"),
        List("arr", "1") -> Data.Str("b"))
    }

    "find nested array indices" in {
      val data = Data.Obj(ListMap(
        "arr" -> Data.Arr(List(
          Data.Obj(ListMap("a" -> Data.Str("foo"))), 
          Data.Obj(ListMap("b" -> Data.Str("bar")))))))
      Values.flatten(data) must_== ListMap(
        List("arr", "0", "a") -> Data.Str("foo"),
        List("arr", "1", "b") -> Data.Str("bar"))
    }
  }
  
  "Values.renderSimple" should {
    implicit val codec = DataCodec.Readable
    
    "render Str without quotes" in {
      Values.renderSimple(Data.Str("abc")) must_== "abc"
    }
    
    "render round Dec without trailing zero" in {
      Values.renderSimple(Data.Dec(1.0)) must_== "1"
    }
    
    "render Timestamp" in {
      val now = Instant.now
      Values.renderSimple(Data.Timestamp(now)) must_== now.toString
    }
  }
}