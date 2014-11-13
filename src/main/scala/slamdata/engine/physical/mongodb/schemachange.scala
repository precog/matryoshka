package slamdata.engine.physical.mongodb

import collection.immutable.ListMap
import slamdata.engine.fp._
import slamdata.engine.javascript._

import scalaz._
import Scalaz._

sealed trait SchemaChange {
  import SchemaChange._
  import ExprOp.DocVar
  import Workflow._
  import IdHandling._

  def nestedField: Option[String] = this match {
    case MakeObject(fields) if fields.size == 1 => fields.headOption.map(_._1)
    case _ => None
  }

  def isNestedField: Boolean = !nestedField.isEmpty

  def nestedArray: Option[Int] = this match {
    case MakeArray(elements) if elements.size == 1 =>
      elements.headOption.map(_._1)
    case _ => None
  }

  def isNestedArray: Boolean = !nestedArray.isEmpty

  def makeObject(name: String): SchemaChange =
    SchemaChange.makeObject(name -> this)

  def makeArray(index: Int): SchemaChange =
    SchemaChange.makeArray(index -> this)

  def replicate: Option[DocVar \/ $Project[Unit]] = toProject.map(_.map(_.id))

  def get(field: BsonField): Option[ExprOp \/ Reshape] = {
    toProject.flatMap(_.fold(d => Some(-\/ (d \ field)), p => p.get(DocVar.ROOT(field))))
  }

  def toJs(base: Js.Expr): Js.Expr = this match {
    case Init => base
    case IndexProject(s, i) => Js.Access(s.toJs(base), Js.Num(i, false))
    case MakeObject(fs)     =>
      Js.AnonObjDecl(fs.map { case (x, y) => (x, y.toJs(base)) }.toList)
    case MakeArray(is)      => Js.AnonElem(is.toList.map(_._2.toJs(base)))
  }

  private def toProject: Option[DocVar \/ $Project[Unit]] = {
    def recurseProject(f1: BsonField, s: SchemaChange):
        Option[DocVar \/ Reshape] =
      for {
        either <- loop(s)
        rez    <- either.fold(
          expr    =>  None,
          reshape =>  (reshape \ f1).map(_.fold(
            e => -\/ (e.asInstanceOf[DocVar]),
            \/- apply)))
      } yield rez
    

    def loop(v: SchemaChange): Option[DocVar \/ Reshape] = v match {
      case Init               => Some(-\/ (DocVar.ROOT()))
      case IndexProject(s, f) => recurseProject(BsonField.Index(f), s)

      case MakeObject(fs) =>
        type MapString[X] = ListMap[String, X]

        for {
          fs  <-  Traverse[MapString].sequence(fs.map(t => t._1 -> loop(t._2)))
        } yield {
          \/-(Reshape.Doc((fs.map {
            case (name, either) =>
              BsonField.Name(name) -> either.fold(
                expr    => -\/(expr),
                reshape => \/-(reshape))
          })))
        }

      case MakeArray(es) =>
        type MapInt[X] = ListMap[Int, X]

        for {
          fs  <-  Traverse[MapInt].sequence(es.map(t => t._1 -> loop(t._2)))
        } yield {
          \/- (Reshape.Arr((fs.map {
            case (index, either) =>
              BsonField.Index(index) -> either.fold(
                expr    => -\/(expr),
                reshape => \/-(reshape))
          })))
        }
    }

    loop(this).map(_.map($Project((), _, IgnoreId)))
  }

  def projectIndex(index: Int): SchemaChange = IndexProject(this, index)

  def simplify: SchemaChange = {
    def simplify0(v: SchemaChange): Option[SchemaChange] = v match {
      case Init => None

      case IndexProject(MakeArray(m), index) if (m.contains(index)) => 
        val child = m(index)

        Some(simplify0(child).flatMap(simplify0 _).getOrElse(child))

      case IndexProject(s, index) => simplify0(s).map(IndexProject(_, index))

      case MakeObject(m) => 
        type MapString[X] = ListMap[String, X]

        Traverse[MapString].sequence(m.map(t => t._1 -> simplify0(t._2))).map(MakeObject.apply)

      case MakeArray(m)  => 
        type MapInt[X] = ListMap[Int, X]

        Traverse[MapInt].sequence(m.map(t => t._1 -> simplify0(t._2))).map(MakeArray.apply)
    }

    simplify0(this).getOrElse(this)
  }

  def isObject = this.simplify match {
    case MakeObject(_) => true
    case _             => false
  }

  def isArray = this.simplify match {
    case MakeArray(_) => true
    case _            => false
  }

  def concat(that: SchemaChange): Option[SchemaChange] =
    (this.simplify, that.simplify) match {
      case (MakeObject(m1), MakeObject(m2)) => Some(MakeObject(m1 ++ m2))
      case (MakeArray(m1),  MakeArray(m2))  => Some(MakeArray(m1 ++ m2))
      case _                                => None
    }

  def subsumes(that: SchemaChange): Boolean =
    (this.simplify, that.simplify) match {
      case (x, y) if x == y => true
      case (MakeObject(m1), MakeObject(m2)) =>
        m2.forall(t2 =>
          m1.exists(t1 => t2._1 == t1._1 && t1._2.subsumes(t2._2)))
      case (MakeArray(m1), MakeArray(m2)) =>
        m2.forall(t2 =>
          m1.exists(t1 => t2._1 == t1._1 && t1._2.subsumes(t2._2)))
      case (IndexProject(s1, v1), IndexProject(s2, v2)) =>
        v1 == v2 && s1.subsumes(s2)
      case _ => false
    }

  def rebase(base: SchemaChange): SchemaChange = this match {
    case Init               => base
    case IndexProject(s, f) => IndexProject(s.rebase(base), f)
    case MakeObject(fs)     =>
      MakeObject(fs.map(t => t._1 -> t._2.rebase(base)))
    case MakeArray(es)      =>
      MakeArray(es.map(t => t._1 -> t._2.rebase(base)))
  }

  def patchField(base: SchemaChange): BsonField => Option[BsonField.Root \/ BsonField] = f => patch(base)(\/- (f))

  def patchRoot(base: SchemaChange): Option[BsonField.Root \/ BsonField] = patch(base)(-\/ (BsonField.Root))

  def patch(base: SchemaChange):
      (BsonField.Root \/ BsonField) => Option[BsonField.Root \/ BsonField] = {
    def patchField0(v: SchemaChange, f: BsonField):
        Option[BsonField.Root \/ BsonField] =
      if (v.subsumes(base)) Some(\/-(f))
      else v match {
        case Init => None
        case MakeObject(fs) =>
          (fs.map {
            case (name, schema) =>
              val nf = BsonField.Name(name)
              patchField0(schema, f).map(_.fold(_ => \/-(nf), f => \/-(nf \ f)))
          }).collect { case Some(x) => x }.headOption
        case MakeArray(es) =>
          (es.map {
            case (index, schema) =>
              val ni = BsonField.Index(index)
              patchField0(schema, f).map(_.fold(_ => \/-(ni), f => \/-(ni \ f)))
          }).collect { case Some(x) => x }.headOption
        case IndexProject(s, i) =>
          f.flatten match {
            case BsonField.Index(`i`) :: xs =>
              BsonField(xs).map { rest =>
                patchField0(s, rest)
              }.getOrElse(Some(-\/ (BsonField.Root)))
            case _ => None
          }
      }

    def patchRoot0(v: SchemaChange): Option[BsonField.Root \/ BsonField] =
      if (v.subsumes(base)) Some(-\/ (BsonField.Root))
      else v match {
        case Init => None
        case MakeObject(fs) =>
          (fs.map {
            case (name, s) =>
              val f = BsonField.Name(name)
              patchRoot0(s).map(_.fold(_ => f, f \ _)).map(\/- apply)
          }).collect { case Some(x) => x }.headOption
        case MakeArray(es) =>
          (es.map {
            case (index, s) =>
              val f = BsonField.Index(index)
              patchRoot0(s).map(_.fold(_ => f, f \ _)).map(\/- apply)
          }).collect { case Some(x) => x }.headOption
        case _ => None
      }
    
    _.fold(
      root  => patchRoot0(this.simplify),
      field => patchField0(this, field))
  }

  import IdHandling._

  def shift(base: DocVar): Option[(Reshape, IdHandling)] = this match {
    case Init =>
      // TODO: Special-casing ExprVar here won’t be necessary once issue #309 is
      //       fixed.
      base match {
        case ExprVar => None
        case _         =>
          Some(Reshape.Doc(ListMap(ExprName -> -\/(base))) -> ExcludeId)
      }
    case IndexProject(_, i) =>
      Some(Reshape.Doc(ListMap(
          ExprName -> -\/(base \ BsonField.Index(i)))) ->
        ExcludeId)
    case MakeObject(fields) =>
      Some(Reshape.Doc(fields.map {
          case (name, _) =>
            BsonField.Name(name) -> -\/ (base \ BsonField.Name(name))
          }) ->
        (if (fields.exists(_._1 == IdLabel)) IncludeId else ExcludeId))
    case MakeArray(fields) =>
      Some(Reshape.Arr(fields.map {
          case (index, _) =>
            BsonField.Index(index) -> -\/ (base \ BsonField.Index(index))
          }) ->
        ExcludeId)
  }
  
}
object SchemaChange {
  import ExprOp.DocVar

  def makeObject(fields: (String, SchemaChange)*): SchemaChange = MakeObject(ListMap(fields: _*))

  def makeArray(elements: (Int, SchemaChange)*): SchemaChange = MakeArray(ListMap(elements: _*))

  case object Init extends SchemaChange
  final case class IndexProject(source: SchemaChange, index: Int)
      extends SchemaChange
  final case class MakeObject(fields: ListMap[String, SchemaChange])
      extends SchemaChange
  final case class MakeArray(elements: ListMap[Int, SchemaChange])
      extends SchemaChange
}
