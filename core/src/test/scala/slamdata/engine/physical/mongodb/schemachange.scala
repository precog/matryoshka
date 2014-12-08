package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._

import collection.immutable.ListMap

import scalaz._
import Scalaz._

import org.scalacheck._

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

class SchemaChangeSpec extends Specification with ScalaCheck with ArbBsonField with DisjunctionMatchers {
  import ExprOp._
  import SchemaChange._
  import Workflow._
  import Gen._

  implicit val arbMakeObject = Arbitrary(Gen.resize(4, Gen.sized(genMakeObject _)))

  implicit val arbMakeArray = Arbitrary(Gen.resize(4, Gen.sized(genMakeArray _)))

  implicit val arbSchemaChange = Arbitrary(Gen.resize(4, Gen.sized(genSchema _)))

  def genSchema(size0: Int): Gen[SchemaChange] = {
    if (size0 <= 0) genInit
    else {
      val size = size0 - 1

      Gen.oneOf(genIndexProject(size), genMakeObject(size), genMakeArray(size))
    }
  } 

  lazy val genInit: Gen[SchemaChange] = Gen.const(SchemaChange.Init)

  def genIndexProject(size: Int): Gen[SchemaChange] = for {
    src   <- genMakeArray(size)
    index <- Gen.oneOf(src.elements.keys.toList)
  } yield SchemaChange.IndexProject(src, index)

  def genMakeObject(size: Int): Gen[SchemaChange.MakeObject] = for {
    list <- nonEmptyListOf(for {
              c    <- alphaChar
              name <- alphaStr
              src  <- genSchema(size)
            } yield (c.toString + name, src))
  } yield SchemaChange.MakeObject(ListMap(list: _*))

  def genMakeArray(size: Int): Gen[SchemaChange.MakeArray] = for {
    list <- nonEmptyListOf(genSchema(size))
  } yield SchemaChange.MakeArray(ListMap((list.zipWithIndex.map(t => t._2 -> t._1)): _*))

  "SchemaChange.subsumes" should {
    "every change subsumes itself" ! prop { (c: SchemaChange) =>
      c.subsumes(c) must beTrue
    }

    "concatenation of objects should subsume right hand side" ! prop { (c1: SchemaChange.MakeObject, c2: SchemaChange.MakeObject) =>
      c1.concat(c2).map(_.subsumes(c2)) must (beSome(true))
    }

    "concatenation of arrays should subsume right hand side" ! prop { (c1: SchemaChange.MakeArray, c2: SchemaChange.MakeArray) =>
      // TODO: Is this what we want???
      c1.concat(c2).map(_.subsumes(c2)) must (beSome(true))
    }
  }
  

  "SchemaChange.patch" should {
    "not change any field when base and delta schema are Init" ! prop { (f: BsonField) =>
      SchemaChange.Init.patchField(SchemaChange.Init)(f) must (beSome(\/- (f)))
    }

    "nest variable when make object is applied" ! prop { (base: SchemaChange.MakeObject, name: String, f: BsonField) =>
      val c = SchemaChange.makeObject(name -> base)

      c.patchField(base)(f) must (beSome(\/- (BsonField.Name(name) \ f)))
    }

    "nest root when make object is applied" ! prop { (base: SchemaChange.MakeObject, name: String) =>
      val c = base.makeObject(name)

      c.patchRoot(base) must (beSome(\/- (BsonField.Name(name))))
    }

    "nest variable when make array is applied" ! prop { (base: SchemaChange.MakeObject, f: BsonField) =>
      val c = SchemaChange.makeArray(0 -> base)

      c.patchField(base)(f) must (beSome(\/- (BsonField.Index(0) \ f)))
    }

    "nest root when make array is applied" ! prop { (base: SchemaChange.MakeObject) =>
      val c = base.makeArray(0)

      c.patchRoot(base) must (beSome(\/- (BsonField.Index(0))))
    }

    "patch variable to root when array project is applied to element in array" ! prop { (base0: SchemaChange) =>
      val base = SchemaChange.makeArray(0 -> base0)

      val c = base.projectIndex(0)

      c.patchField(base)(BsonField.Index(0)) must (beSome(-\/ (BsonField.Root)))
    }

    "patch variable to projected element" ! prop { (base0: SchemaChange) =>
      val base1 = SchemaChange.makeArray(0 -> base0)
      val base2 = SchemaChange.makeArray(0 -> base1)

      val c = base2.projectIndex(0)

      c.patchField(base2)(BsonField.Index(0) \ BsonField.Index(0)) must (beSome(\/- (BsonField.Index(0))))
    }

    "not patch variable when not in projected path" ! prop { (base0: SchemaChange) =>
      val base1 = SchemaChange.makeArray(0 -> base0)
      val base2 = SchemaChange.makeArray(0 -> base1)

      val c = base2.projectIndex(0)

      c.patchField(base2)(BsonField.Index(1) \ BsonField.Index(0)) must beNone
    }

    "not patch root when element project is applied" ! prop { (base0: SchemaChange) =>
      val base1 = SchemaChange.makeArray(0 -> base0)

      val c = base1.projectIndex(0)

      c.patchRoot(base1) must beNone
    }

    "patch root relative to base schema" ! prop { (base1: SchemaChange.MakeObject, name: String) =>
      val base2 = base1.makeObject(name)

      base1.patchRoot(SchemaChange.Init).flatMap(_.toOption).map { oldRoot =>
        val newRoot = base2.patchRoot(base1).flatMap(_.toOption).get

        newRoot must_== BsonField.Name(name)
      }.getOrElse(1 must_== 1)
    }

    "patch root to one level nested" ! prop { (base1: SchemaChange, name: String) =>
      val base2 = base1.makeObject(name)

      base1.patchRoot(SchemaChange.Init).flatMap(_.toOption).map { oldRoot =>
        val newRoot = base2.patchRoot(SchemaChange.Init).flatMap(_.toOption).get

        newRoot must_== (BsonField.Name(name) \ oldRoot)
      }.getOrElse(1 must_== 1)
    }
  }

  "SchemaChange.replicate" should {
    "return nesting function on Init input" in {
      val foo = BsonField.Name("foo")

      SchemaChange.Init.replicate must beSome(-\/(DocVar.ROOT()))
    }

    "create proper projection for doubly object-nested Init" ! prop { (name1: String, name2: String) =>
      val field1 = BsonField.Name(name1)
      val field2 = BsonField.Name(name2)

      val object1 = SchemaChange.makeObject(name1 -> SchemaChange.Init)
      val object2 = SchemaChange.makeObject(name2 -> object1)

      object2.replicate must
      beSome(\/-($Project((),
        Reshape.Doc(ListMap(
          field2 -> \/- (Reshape.Doc(ListMap(
            field1 -> -\/(ExprOp.DocField(field2 \ field1))))))),
        if (field1 == Workflow.IdName || field2 == Workflow.IdName)
          IdHandling.IncludeId
        else IdHandling.IgnoreId)))
    }
  }
}
