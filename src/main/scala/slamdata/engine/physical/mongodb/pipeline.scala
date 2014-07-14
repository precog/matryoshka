package slamdata.engine.physical.mongodb

import scala.collection.immutable.{ListMap}

import com.mongodb.DBObject

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, RenderedTree}
import slamdata.engine.fp._

final case class Pipeline(ops: List[PipelineOp]) {
  def repr: java.util.List[DBObject] = ops.foldLeft(new java.util.ArrayList[DBObject](): java.util.List[DBObject]) {
    case (list, op) =>
      list.add(op.bson.repr)

      list
  }

  def reverse: Pipeline = copy(ops = ops.reverse)

  def merge(that: Pipeline): PipelineMergeError \/ Pipeline = mergeM[Free.Trampoline](that).run.map(_._1).map(Pipeline(_))

  private def mergeM[F[_]](that: Pipeline)(implicit F: Monad[F]): F[PipelineMergeError \/ (List[PipelineOp], MergePatch, MergePatch)] = {
    PipelineMerge.mergeOpsM[F](Nil, this.ops, MergePatch.Id, that.ops, MergePatch.Id).run
  }
}
object Pipeline {
  implicit def PipelineRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[Pipeline] {
    override def render(p: Pipeline) = NonTerminal("Pipeline", p.ops.map(RO.render(_)))
  }
}

sealed trait PipelineOp {
  def bson: Bson.Doc

  def isShapePreservingOp: Boolean = this match {
    case x : PipelineOp.ShapePreservingOp => true
    case _ => false
  }

  def isNotShapePreservingOp: Boolean = !isShapePreservingOp

  def commutesWith(that: PipelineOp): Boolean = false
}

object PipelineOp {
  sealed trait ShapePreservingOp extends PipelineOp

  implicit def PipelineOpRenderTree(implicit RG: RenderTree[Grouped], RS: RenderTree[Selector]) = new RenderTree[PipelineOp] {
    def render(op: PipelineOp) = op match {
      case Project(Reshape.Doc(map)) => renderReshape("Project", map)
      case Project(Reshape.Arr(map)) => renderReshape("Project", map)
      case Group(grouped, by)        => NonTerminal("Group", RG.render(grouped) :: Terminal(by.toString) :: Nil)
      case Match(selector)           => NonTerminal("Match", RS.render(selector) :: Nil)
      case Sort(keys)                => NonTerminal("Sort", (keys.map { case (expr, ot) => Terminal(expr + " -> " + ot) } ).toList)
      case _                         => Terminal(op.toString)
    }
  }

  private def renderReshape[A <: BsonField.Leaf](label: String, map: Map[A, ExprOp \/ Reshape]): RenderedTree = {
    val ReshapeRenderTree: RenderTree[(BsonField, ExprOp \/ Reshape)] = new RenderTree[(BsonField, ExprOp \/ Reshape)] {
      override def render(v: (BsonField, ExprOp \/ Reshape)) = v match {
        case (field, -\/  (exprOp))  => Terminal(field + " -> " + exprOp.toString)
        case (field,  \/- (Reshape.Doc(map))) => renderReshape(field.toString, map)
        case (field,  \/- (Reshape.Arr(map))) => renderReshape(field.toString, map)
      }
    }

    NonTerminal(label, map.map(ReshapeRenderTree.render).toList)
  }

  implicit def GroupedRenderTree = new RenderTree[Grouped] {
    def render(grouped: Grouped) = NonTerminal("Grouped", (grouped.value.map { case (name, expr) => Terminal(name + " -> " + expr) } ).toList)
  }
  
  private[PipelineOp] abstract sealed class SimpleOp(op: String) extends PipelineOp {
    def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  sealed trait Reshape {
    def toDoc: Reshape.Doc

    def bson: Bson.Doc

    def schema: PipelineSchema.Succ

    def nestField(name: String): Reshape.Doc = Reshape.Doc(Map(BsonField.Name(name) -> \/-(this)))

    def nestIndex(index: Int): Reshape.Arr = Reshape.Arr(Map(BsonField.Index(index) -> \/-(this)))

    def ++ (that: Reshape): Reshape = {
      implicit val sg = Semigroup.lastSemigroup[ExprOp \/ Reshape]

      (this, that) match {
        case (Reshape.Arr(m1), Reshape.Arr(m2)) => Reshape.Arr(m1 |+| m2)

        case (r1_, r2_) => 
          val r1 = r1_.toDoc 
          val r2 = r2_.toDoc

          Reshape.Doc(r1.value |+| r2.value)
      }
    }

  }

  object Reshape {
    def unapply(v: Reshape): Option[Reshape] = Some(v)
    
    case class Doc(value: Map[BsonField.Name, ExprOp \/ Reshape]) extends Reshape {
      def schema: PipelineSchema.Succ = PipelineSchema.Succ(value.map {
        case (n, v) => (n: BsonField.Leaf) -> v.fold(_ => -\/ (()), r => \/-(r.schema))
      })

      def bson: Bson.Doc = Bson.Doc(value.map {
        case (field, either) => field.asText -> either.fold(_.bson, _.bson)
      })

      def toDoc = this

      override def toString = s"Reshape.Doc($value)"
    }
    case class Arr(value: Map[BsonField.Index, ExprOp \/ Reshape]) extends Reshape {      
      def schema: PipelineSchema.Succ = PipelineSchema.Succ(value.map {
        case (n, v) => (n: BsonField.Leaf) -> v.fold(_ => -\/ (()), r => \/-(r.schema))
      })

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

      // def flatten: (Map[BsonField.Index, ExprOp], Reshape.Arr)

      override def toString = s"Reshape.Arr($value)"
    }

    implicit val ReshapeMonoid = new Monoid[Reshape] {
      def zero = Reshape.Arr(Map())

      def append(v10: Reshape, v20: => Reshape): Reshape = {
        val v1 = v10.toDoc
        val v2 = v20.toDoc

        val m1 = v1.value
        val m2 = v2.value
        val keys = m1.keySet ++ m2.keySet

        Reshape.Doc(keys.foldLeft(Map.empty[BsonField.Name, ExprOp \/ Reshape]) {
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
  }

  case class Grouped(value: Map[BsonField.Leaf, ExprOp.GroupOp]) {
    def schema: PipelineSchema.Succ = PipelineSchema.Succ(value.mapValues(_ => -\/(())))

    def bson = Bson.Doc(value.map(t => t._1.asText -> t._2.bson))
  }
  
  case class Project(shape: Reshape) extends SimpleOp("$project") {
    def rhs = shape.bson

    def schema: PipelineSchema = shape.schema

    def id: Project = {
      def loop(prefix: Option[BsonField], p: Project): Project = {
        def nest(child: BsonField): BsonField = prefix.map(_ \ child).getOrElse(child)

        Project(p.shape match {
          case Reshape.Doc(m) => 
            Reshape.Doc(
              m.transform {
                case (k, v) =>
                  v.fold(
                    _ => -\/  (ExprOp.DocVar.ROOT(nest(k))),
                    r =>  \/- (loop(Some(nest(k)), Project(r)).shape)
                  )
              }
            )

          case Reshape.Arr(m) =>
            Reshape.Arr(
              m.transform {
                case (k, v) =>
                  v.fold(
                    _ => -\/  (ExprOp.DocVar.ROOT(nest(k))),
                    r =>  \/- (loop(Some(nest(k)), Project(r)).shape)
                  )
              }
            )
        })
      }

      loop(None, this)
    }

    def nestField(name: String): Project = Project(shape.nestField(name))

    def nestIndex(index: Int): Project = Project(shape.nestIndex(index))

    def ++ (that: Project): Project = Project(this.shape ++ that.shape)

    def field(name: String): Option[ExprOp \/ Project] = shape match {
      case Reshape.Doc(m) => m.get(BsonField.Name(name)).map { _ match {
          case e @ -\/(_) => e
          case     \/-(r) => \/- (Project(r))
        }
      }

      case _ => None
    }

    def index(idx: Int): Option[ExprOp \/ Project] = shape match {
      case Reshape.Arr(m) => m.get(BsonField.Index(idx)).map { _ match {
          case e @ -\/(_) => e
          case     \/-(r) => \/- (Project(r))
        }
      }

      case _ => None
    }
  }
  case class Match(selector: Selector) extends SimpleOp("$match") with ShapePreservingOp {
    def rhs = selector.bson
  }
  case class Redact(value: ExprOp) extends SimpleOp("$redact") {
    def rhs = value.bson

    def fields: List[ExprOp.DocVar] = {
      import scalaz.std.list._

      ExprOp.foldMap({
        case f : ExprOp.DocVar => f :: Nil
      })(value)
    }
  }

  object Redact {
    val DESCEND = ExprOp.DocVar(ExprOp.DocVar.Name("DESCEND"),  None)
    val PRUNE   = ExprOp.DocVar(ExprOp.DocVar.Name("PRUNE"),    None)
    val KEEP    = ExprOp.DocVar(ExprOp.DocVar.Name("KEEP"),     None)
  }
  
  case class Limit(value: Long) extends SimpleOp("$limit") with ShapePreservingOp {
    def rhs = Bson.Int64(value)
  }
  case class Skip(value: Long) extends SimpleOp("$skip") with ShapePreservingOp {
    def rhs = Bson.Int64(value)
  }
  case class Unwind(field: ExprOp.DocVar) extends SimpleOp("$unwind") {
    def rhs = Bson.Text(field.field.asField)
  }
  case class Group(grouped: Grouped, by: ExprOp \/ Reshape) extends SimpleOp("$group") {
    def schema: PipelineSchema = grouped.schema

    def rhs = {
      val Bson.Doc(m) = grouped.bson

      Bson.Doc(m + ("_id" -> by.fold(_.bson, _.bson)))
    }
  }
  case class Sort(value: NonEmptyList[(BsonField, SortType)]) extends SimpleOp("$sort") with ShapePreservingOp {
    // Note: ListMap preserves the order of entries.
    def rhs = Bson.Doc(ListMap((value.map { case (k, t) => k.asText -> t.bson }).list: _*))
    
    override def toString = "Sort(NonEmptyList(" + value.map(t => t._1 + " -> " + t._2).list.mkString(", ") + "))"
  }
  case class Out(collection: Collection) extends SimpleOp("$out") with ShapePreservingOp {
    def rhs = Bson.Text(collection.name)
  }
  case class GeoNear(near: (Double, Double), distanceField: BsonField, 
                     limit: Option[Int], maxDistance: Option[Double],
                     query: Option[FindQuery], spherical: Option[Boolean],
                     distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
                     uniqueDocs: Option[Boolean]) extends SimpleOp("$geoNear") {
    def rhs = Bson.Doc(List(
      List("near"           -> Bson.Arr(Bson.Dec(near._1) :: Bson.Dec(near._2) :: Nil)),
      List("distanceField"  -> distanceField.bson),
      limit.toList.map(limit => "limit" -> Bson.Int32(limit)),
      maxDistance.toList.map(maxDistance => "maxDistance" -> Bson.Dec(maxDistance)),
      query.toList.map(query => "query" -> query.bson),
      spherical.toList.map(spherical => "spherical" -> Bson.Bool(spherical)),
      distanceMultiplier.toList.map(distanceMultiplier => "distanceMultiplier" -> Bson.Dec(distanceMultiplier)),
      includeLocs.toList.map(includeLocs => "includeLocs" -> includeLocs.bson),
      uniqueDocs.toList.map(uniqueDocs => "uniqueDocs" -> Bson.Bool(uniqueDocs))
    ).flatten.toMap)
  }
}
