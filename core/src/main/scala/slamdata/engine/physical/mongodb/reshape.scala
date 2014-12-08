package slamdata.engine.physical.mongodb

import scala.collection.immutable.{ListMap}

import scalaz._
import Scalaz._

import slamdata.engine.{Error, RenderTree, Terminal, NonTerminal, RenderedTree}
import slamdata.engine.fp._
import slamdata.engine.javascript._

case class Grouped(value: ListMap[BsonField.Leaf, ExprOp.GroupOp]) {
  type LeafMap[V] = ListMap[BsonField.Leaf, V]

  def bson = Bson.Doc(value.map(t => t._1.asText -> t._2.bson))

  override def toString = s"Grouped(List$value)"

  def rewriteRefs(f: PartialFunction[ExprOp.DocVar, ExprOp.DocVar]): Grouped =
    Grouped(value.transform {
      case (k, groupOp) => groupOp.rewriteRefs(f) match {
        case groupOp: ExprOp.GroupOp => groupOp
        case _ => sys.error("Transformation changed the type -- error!")
      }
    })
}
object Grouped {
  implicit def GroupedRenderTree = new RenderTree[Grouped] {
    val GroupedNodeType = List("Grouped")

    def render(grouped: Grouped) = NonTerminal("",
                                    (grouped.value.map { case (name, expr) => Terminal(name.bson.repr.toString + " -> " + expr.bson.repr.toString, GroupedNodeType :+ "Name") } ).toList, 
                                    GroupedNodeType)
  }
}

sealed trait Reshape {
  def toDoc: Reshape.Doc
  def toJs: Error \/ JsMacro

  def bson: Bson.Doc

  private def projectSeq(fs: List[BsonField.Leaf]): Option[ExprOp \/ Reshape] = fs match {
    case Nil => Some(\/- (this))
    case (x : BsonField.Leaf) :: Nil => this.project(x)
    case (x : BsonField.Leaf) :: xs => this.project(x).flatMap(_.fold(
      expr    => None,
      reshape => reshape.projectSeq(xs)
    ))
  }

  def rewriteRefs(applyVar: PartialFunction[ExprOp.DocVar, ExprOp.DocVar]):
      Reshape =
    this match {
      case Reshape.Doc(value) => Reshape.Doc(value.transform {
        case (_, -\/(e)) => -\/(e.rewriteRefs(applyVar))
        case (_, \/-(r)) => \/-(r.rewriteRefs(applyVar))
      })
      case Reshape.Arr(value) => Reshape.Arr(value.transform {
        case (_, -\/(e)) => -\/(e.rewriteRefs(applyVar))
        case (_, \/-(r)) => \/-(r.rewriteRefs(applyVar))
      })
    }

  def \ (f: BsonField): Option[ExprOp \/ Reshape] = projectSeq(f.flatten)

  private def project(leaf: BsonField.Leaf): Option[ExprOp \/ Reshape] = leaf match {
    case x @ BsonField.Name(_) => projectField(x)
    case x @ BsonField.Index(_) => projectIndex(x)
  }

  private def projectField(f: BsonField.Name): Option[ExprOp \/ Reshape] = this match {
    case Reshape.Doc(m) => m.get(f)
    case Reshape.Arr(_) => None
  }

  private def projectIndex(f: BsonField.Index): Option[ExprOp \/ Reshape] = this match {
    case Reshape.Doc(_) => None
    case Reshape.Arr(m) => m.get(f)
  }

  def get(field: BsonField): Option[ExprOp \/ Reshape] = {
    def get0(cur: Reshape, els: List[BsonField.Leaf]): Option[ExprOp \/ Reshape] = els match {
      case Nil => ???

      case x :: Nil => cur.toDoc.value.get(x.toName)

      case x :: xs => cur.toDoc.value.get(x.toName).flatMap(_.fold(_ => None, get0(_, xs)))
    }

    get0(this, field.flatten)
  }

  def set(field: BsonField, newv: ExprOp \/ Reshape): Reshape = {
    def getOrDefault(o: Option[ExprOp \/ Reshape]): Reshape = {
      o.map(_.fold(_ => Reshape.EmptyArr, identity)).getOrElse(Reshape.EmptyArr)
    }

    def set0(cur: Reshape, els: List[BsonField.Leaf]): Reshape = els match {
      case Nil => ??? // TODO: Refactor els to be NonEmptyList

      case (x : BsonField.Name) :: Nil => Reshape.Doc(cur.toDoc.value + (x -> newv))

      case (x : BsonField.Index) :: Nil => cur match {
        case Reshape.Arr(m) => Reshape.Arr(m + (x -> newv))
        case Reshape.Doc(m) => Reshape.Doc(m + (x.toName -> newv))
      }

      case (x : BsonField.Name) :: xs =>
        val doc = cur.toDoc.value

        Reshape.Doc(doc + (x -> \/- (set0(getOrDefault(doc.get(x)), xs))))

      case (x : BsonField.Index) :: xs => cur match {
        case Reshape.Arr(m) => Reshape.Arr(m + (x -> \/- (set0(getOrDefault(m.get(x)), xs))))
        case Reshape.Doc(m) => Reshape.Doc(m + (x.toName -> \/- (set0(getOrDefault(m.get(x.toName)), xs))))
      }
    }

    set0(this, field.flatten)
  }
}

object Reshape {
  val EmptyArr = Reshape.Arr(ListMap())
  val EmptyDoc = Reshape.Doc(ListMap())

  def unapply(v: Reshape): Option[Reshape] = Some(v)

  def getAll(r: Reshape): List[(BsonField, ExprOp)] = {
    def getAll0(f0: BsonField, e: ExprOp \/ Reshape) = e.fold(
      e => (f0 -> e) :: Nil,
      r => getAll(r).map { case (f, e) => (f0 \ f) -> e })

    r match {
      case Reshape.Arr(m) =>
        m.toList.map { case (f, e) => getAll0(f, e) }.flatten
      case Reshape.Doc(m) =>
        m.toList.map { case (f, e) => getAll0(f, e) }.flatten
    }
  }

  def setAll(r: Reshape, fvs: Iterable[(BsonField, ExprOp \/ Reshape)]) =
    fvs.foldLeft(r) {
      case (r0, (field, value)) => r0.set(field, value)
    }

  def mergeMaps[A, B](lmap: ListMap[A, B], rmap: ListMap[A, B]):
      Option[ListMap[A, B]] =
    if ((lmap.keySet & rmap.keySet).forall(k => lmap.get(k) == rmap.get(k)))
      Some(lmap ++ rmap)
    else None

  def merge(r1: Reshape, r2: Reshape): Option[Reshape] = (r1, r2) match {
    case (Reshape.Doc(_), Reshape.Doc(_)) =>
      val lmap = Reshape.getAll(r1).map(t => t._1 -> -\/(t._2)).toListMap
      val rmap = Reshape.getAll(r2).map(t => t._1 -> -\/(t._2)).toListMap
      if ((lmap.keySet & rmap.keySet).forall(k => lmap.get(k) == rmap.get(k)))
        Some(Reshape.setAll(
          r1,
          Reshape.getAll(r2).map(t => t._1 -> -\/ (t._2))))
      else None
    // TODO: Attempt to merge Arr+Arr as well
    case _ => None
  }

  case class Doc(value: ListMap[BsonField.Name, ExprOp \/ Reshape]) extends Reshape {
    def bson: Bson.Doc = Bson.Doc(value.map {
      case (field, either) => field.asText -> either.fold(_.bson, _.bson)
    })

    def toDoc = this

    def toJs = 
      value.map { case (key, expr) =>
        key.asText -> expr.fold(ExprOp.toJs(_), _.toJs)
      }.sequenceU.map { l => JsMacro { base => 
        JsCore.Obj(l.map { case (k, v) => k -> v(base) }).fix } }

    override def toString = s"Reshape.Doc(List$value)"
  }
  case class Arr(value: ListMap[BsonField.Index, ExprOp \/ Reshape]) extends Reshape {
    def bson: Bson.Doc = Bson.Doc(value.map {
      case (field, either) => field.asText -> either.fold(_.bson, _.bson)
    })

    def minIndex: Option[Int] = {
      val keys = value.keys

      keys.headOption.map(_ => keys.map(_.value).min)
    }

    def maxIndex: Option[Int] = {
      val keys = value.keys

      keys.headOption.map(_ => keys.map(_.value).max)
    }

    def offset(i0: Int) = Reshape.Arr(value.map {
      case (BsonField.Index(i), v) => BsonField.Index(i0 + i) -> v
    })

    def toDoc: Doc = Doc(value.map(t => t._1.toName -> t._2))
    def toJs = toDoc.toJs  // NB: generating an actual array would be tricky since the keys may not be contiguous

    // def flatten: (Map[BsonField.Index, ExprOp], Reshape.Arr)

    override def toString = s"Reshape.Arr(List$value)"
  }
  implicit val ReshapeMonoid = new Monoid[Reshape] {
    def zero = Reshape.Arr(ListMap.empty)

    def append(v10: Reshape, v20: => Reshape): Reshape = {
      val v1 = v10.toDoc
      val v2 = v20.toDoc

      val m1 = v1.value
      val m2 = v2.value
      val keys = m1.keySet ++ m2.keySet

      Reshape.Doc(keys.foldLeft(ListMap.empty[BsonField.Name, ExprOp \/ Reshape]) {
        case (map, key) =>
          val left  = m1.get(key)
          val right = m2.get(key)

          val result = ((left |@| right) {
            case (-\/(e1), -\/(e2)) => -\/ (e2)
            case (-\/(e1), \/-(r2)) => \/- (r2)
            case (\/-(r1), \/-(r2)) => \/- (append(r1, r2))
            case (\/-(r1), -\/(e2)) => -\/ (e2)
          }) orElse (left) orElse (right)

          map + (key -> result.get)
      })
    }
  }

  private val PipelineOpNodeType = List("PipelineOp")
  private val ProjectNodeType = List("PipelineOp", "Project")
  private val SortNodeType = List("PipelineOp", "Sort")
  private val SortKeyNodeType = List("PipelineOp", "Sort", "Key")

  private[mongodb] def renderReshape(shape: Reshape): List[RenderedTree] = {
    def renderField(field: BsonField, value: ExprOp \/ Reshape) = {
      val (label, typ) = field match {
        case BsonField.Index(value) => value.toString -> "Index"
        case _ => field.bson.repr.toString -> "Name"
      }
      value match {
        case -\/  (exprOp) => Terminal(label + " -> " + exprOp.bson.repr.toString, ProjectNodeType :+ typ)
        case  \/- (shape)  => NonTerminal(label, renderReshape(shape), ProjectNodeType :+ typ)
      }
    }

    val fields = shape match { case Reshape.Doc(map) => map; case Reshape.Arr(map) => map }
    fields.map { case (k, v) => renderField(k, v) }.toList
  }

}
