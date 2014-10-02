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
}
object Pipeline {
  implicit def PipelineRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[Pipeline] {
    override def render(p: Pipeline) = NonTerminal("", 
                                        p.ops.map(RO.render(_)),
                                        "Pipeline" :: Nil)
  }
}

sealed trait PipelineOp {
  import PipelineOp._
  import ExprOp._

  def bson: Bson.Doc

  def isShapePreservingOp: Boolean = this match {
    case x : PipelineOp.ShapePreservingOp => true
    case _ => false
  }

  def isNotShapePreservingOp: Boolean = !isShapePreservingOp

  def schema: SchemaChange

  def rewriteRefs(applyVar0: PartialFunction[DocVar, DocVar]): this.type = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyExprOp(e: ExprOp): ExprOp = e.mapUp {
      case f : DocVar => applyVar(f)
    }

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyReshape(shape: Reshape): Reshape = shape match {
      case Reshape.Doc(value) => Reshape.Doc(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })

      case Reshape.Arr(value) => Reshape.Arr(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })
    }

    def applyGrouped(grouped: Grouped): Grouped = Grouped(grouped.value.transform {
      case (k, groupOp) => applyExprOp(groupOp) match {
        case groupOp : GroupOp => groupOp
        case _ => sys.error("Transformation changed the type -- error!")
      }
    })

    def applyMap[A](m: ListMap[BsonField, A]): ListMap[BsonField, A] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyNel[A](m: NonEmptyList[(BsonField, A)]): NonEmptyList[(BsonField, A)] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyFindQuery(q: FindQuery): FindQuery = {
      q.copy(
        query   = applySelector(q.query),
        max     = q.max.map(applyMap _),
        min     = q.min.map(applyMap _),
        orderby = q.orderby.map(applyNel _)
      )
    }

    (this match {
      case Project(shape)     => Project(applyReshape(shape))
      case Group(grouped, by) => Group(applyGrouped(grouped), by.bimap(applyExprOp _, applyReshape _))
      case Match(s)           => Match(applySelector(s))
      case Redact(e)          => Redact(applyExprOp(e))
      case v @ Limit(_)       => v
      case v @ Skip(_)        => v
      case v @ Unwind(f)      => Unwind(applyVar(f))
      case v @ Sort(l)        => Sort(applyNel(l))
      case v @ Out(_)         => v
      case g : GeoNear        => g.copy(distanceField = applyFieldName(g.distanceField), query = g.query.map(applyFindQuery _))
    }).asInstanceOf[this.type]
  }

  final def refs: List[DocVar] = {
    // Sorry world
    val vf = new scala.collection.mutable.ListBuffer[DocVar]

    rewriteRefs {
      case v => vf += v; v
    }

    vf.toList
  }
}

object PipelineOp {
  sealed trait ShapePreservingOp extends PipelineOp {
    final def schema: SchemaChange = SchemaChange.Init
  }

  private val PipelineOpNodeType = List("PipelineOp")
  private val ProjectNodeType = List("PipelineOp", "Project")
  private val SortNodeType = List("PipelineOp", "Sort")
  private val SortKeyNodeType = List("PipelineOp", "Sort", "Key")
  
  implicit def PipelineOpRenderTree(implicit RG: RenderTree[Grouped], RS: RenderTree[Selector]) = new RenderTree[PipelineOp] {
    def render(op: PipelineOp) = op match {
      case Project(shape)            => renderReshape("Project", "", shape)
      case Group(grouped, by)        => NonTerminal("",
                                          RG.render(grouped) :: 
                                            by.fold(exprOp => Terminal(exprOp.bson.repr.toString, PipelineOpNodeType :+ "Group" :+ "By"), 
                                                    shape => renderReshape("By", "", shape)) ::
                                            Nil, 
                                          PipelineOpNodeType :+ "Group")
      case Match(selector)           => NonTerminal("", RS.render(selector) :: Nil, PipelineOpNodeType :+ "Match")
      case Sort(keys)                => NonTerminal("", (keys.map { case (expr, ot) => Terminal(expr.bson.repr.toString + ", " + ot, SortKeyNodeType) } ).toList, SortNodeType)
      case Unwind(field)             => Terminal(field.bson.repr.toString, PipelineOpNodeType :+ "Unwind")
      case _                         => Terminal(op.toString, PipelineOpNodeType)
    }
  }

  private[mongodb] def renderReshape[A <: BsonField.Leaf](nodeType: String, label: String, shape: Reshape): RenderedTree = {
    val ReshapeRenderTree: RenderTree[(BsonField, ExprOp \/ Reshape)] = new RenderTree[(BsonField, ExprOp \/ Reshape)] {
      override def render(v: (BsonField, ExprOp \/ Reshape)) = v match {
        case (BsonField.Index(index), -\/  (exprOp)) => Terminal(index.toString + " -> " + exprOp.bson.repr.toString, ProjectNodeType :+ "Index")
        case (field, -\/  (exprOp)) => Terminal(field.bson.repr.toString + " -> " + exprOp.bson.repr.toString, ProjectNodeType :+ "Name")
        case (field,  \/- (shape))  => renderReshape("Shape", field.asText, shape)
      }
    }

    val map = shape match { case Reshape.Doc(map) => map; case Reshape.Arr(map) => map }
    NonTerminal(label, map.map(ReshapeRenderTree.render).toList, ProjectNodeType :+ nodeType)
  }

  implicit def GroupedRenderTree = new RenderTree[Grouped] {
    val GroupedNodeType = List("Grouped")

    def render(grouped: Grouped) = NonTerminal("", 
                                    (grouped.value.map { case (name, expr) => Terminal(name.bson.repr.toString + " -> " + expr.bson.repr.toString, GroupedNodeType :+ "Name") } ).toList, 
                                    GroupedNodeType)
  }
  
  private[PipelineOp] abstract sealed class SimpleOp(op: String) extends PipelineOp {
    def rhs: Bson

    def bson = Bson.Doc(ListMap(op -> rhs))
  }

  sealed trait Reshape {
    def toDoc: Reshape.Doc

    def schema: SchemaChange = {
      def convert(e: ExprOp \/ Reshape): SchemaChange = e.fold({
        case ExprOp.DocVar(_, Some(field)) => 
          field.flatten.foldLeft[SchemaChange](SchemaChange.Init) {
            case (s, BsonField.Name(name)) => s.projectField(name)
            case (s, BsonField.Index(index)) => s.projectIndex(index)
          }

        case _ => SchemaChange.Init
      }, loop _)

      def loop(s: Reshape): SchemaChange = s match {
        case Reshape.Doc(m) => SchemaChange.MakeObject(m.map {
          case (k, v) => k.value -> convert(v)
        })

        case Reshape.Arr(m) => SchemaChange.MakeArray(m.map {
          case (k, v) => k.value -> convert(v)
        })
      }

      loop(this)
    }

    def bson: Bson.Doc

    def nestField(name: String): Reshape.Doc = Reshape.Doc(ListMap(BsonField.Name(name) -> \/-(this)))

    def nestIndex(index: Int): Reshape.Arr = Reshape.Arr(ListMap(BsonField.Index(index) -> \/-(this)))

    private def projectSeq(fs: List[BsonField.Leaf]): Option[ExprOp \/ Reshape] = fs match {
      case Nil => Some(\/- (this))
      case (x : BsonField.Leaf) :: Nil => this.project(x)
      case (x : BsonField.Leaf) :: xs => this.project(x).flatMap(_.fold(
        expr    => None,
        reshape => reshape.projectSeq(xs)
      ))
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

    def ++ (that: Reshape): Reshape = {
      implicit val sg = Semigroup.lastSemigroup[ExprOp \/ Reshape]

      (this, that) match {
        case (Reshape.Arr(m1), Reshape.Arr(m2)) => Reshape.Arr(m1 ++ m2)

        case (r1_, r2_) => 
          val r1 = r1_.toDoc 
          val r2 = r2_.toDoc

          Reshape.Doc(r1.value ++ r2.value)
      }
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
    
    case class Doc(value: ListMap[BsonField.Name, ExprOp \/ Reshape]) extends Reshape {
      def bson: Bson.Doc = Bson.Doc(value.map {
        case (field, either) => field.asText -> either.fold(_.bson, _.bson)
      })

      def toDoc = this

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
  }

  case class Grouped(value: ListMap[BsonField.Leaf, ExprOp.GroupOp]) {
    type LeafMap[V] = ListMap[BsonField.Leaf, V]
    
    def bson = Bson.Doc(value.map(t => t._1.asText -> t._2.bson))

    override def toString = s"Grouped(List$value)"
  }
  
  case class Project(shape: Reshape) extends SimpleOp("$project") {
    def rhs = shape.bson

    def schema = shape.schema

    def empty: Project = shape match {
      case Reshape.Doc(_) => Project.EmptyDoc

      case Reshape.Arr(_) => Project.EmptyArr
    }

    def set(field: BsonField, value: ExprOp \/ Reshape): Project = Project(shape.set(field, value))

    def getAll: List[(BsonField, ExprOp)] = {
      def fromReshape(r: Reshape): List[(BsonField, ExprOp)] = r match {
        case Reshape.Arr(m) => m.toList.map { case (f, e) => getAll0(f, e) }.flatten
        case Reshape.Doc(m) => m.toList.map { case (f, e) => getAll0(f, e) }.flatten
      }

      def getAll0(f0: BsonField, e: ExprOp \/ Reshape): List[(BsonField, ExprOp)] = e.fold(
        e => (f0 -> e) :: Nil,
        r => fromReshape(r).map { case (f, e) => (f0 \ f) -> e }
      )

      fromReshape(shape)
    }

    def get(ref: ExprOp.DocVar): Option[ExprOp \/ Reshape] = ref match {
      case ExprOp.DocVar(_, Some(field)) => shape.get(field)
      case _ => Some(\/- (shape))
    }

    def setAll(fvs: Iterable[(BsonField, ExprOp \/ Reshape)]): Project = fvs.foldLeft(this) {
      case (project, (field, value)) => project.set(field, value)
    }

    def deleteAll(fields: List[BsonField]): Project = {
      empty.setAll(getAll.filterNot(t => fields.exists(t._1.startsWith(_))).map(t => t._1 -> -\/ (t._2)))
    }

    def id: Project = {
      def loop(prefix: Option[BsonField], p: Project): Project = {
        def nest(child: BsonField): BsonField =
          prefix.map(_ \ child).getOrElse(child)

        Project(p.shape match {
          case Reshape.Doc(m) =>
            Reshape.Doc(
              m.transform {
                case (k, v) =>
                  v.fold(
                    _ => -\/  (ExprOp.DocVar.ROOT(nest(k))),
                    r =>  \/- (loop(Some(nest(k)), Project(r)).shape))
              })
          case Reshape.Arr(m) =>
            Reshape.Arr(
              m.transform {
                case (k, v) =>
                  v.fold(
                    _ => -\/  (ExprOp.DocVar.ROOT(nest(k))),
                    r =>  \/- (loop(Some(nest(k)), Project(r)).shape))
              })
        })
      }

      loop(None, this)
    }

    def nestField(name: String): Project = Project(shape.nestField(name))

    def nestIndex(index: Int): Project = Project(shape.nestIndex(index))

    def ++ (that: Project): Project = Project(this.shape ++ that.shape)

    def field(name: String): Option[ExprOp \/ Project] = shape match {
      case Reshape.Doc(m) => m.get(BsonField.Name(name)).map { _ match {
          case e @ -\/  (_) => e
          case      \/- (r) => \/- (Project(r))
        }
      }

      case _ => None
    }

    def index(idx: Int): Option[ExprOp \/ Project] = shape match {
      case Reshape.Arr(m) => m.get(BsonField.Index(idx)).map { _ match {
          case e @ -\/  (_) => e
          case      \/- (r) => \/- (Project(r))
        }
      }

      case _ => None
    }
  }
  object Project {
    import ExprOp.DocVar

    val EmptyDoc = Project(Reshape.EmptyDoc)
    val EmptyArr = Project(Reshape.EmptyArr)   
  }
  case class Match(selector: Selector) extends SimpleOp("$match") with ShapePreservingOp {
    def rhs = selector.bson
  }
  case class Redact(value: ExprOp) extends SimpleOp("$redact") {
    def schema = SchemaChange.Init // FIXME!

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
    def schema = SchemaChange.Init // FIXME

    def rhs = field.bson
  }
  case class Group(grouped: Grouped, by: ExprOp \/ Reshape) extends SimpleOp("$group") {
    import ExprOp.{DocVar, GroupOp}

    def schema = SchemaChange.MakeObject(grouped.value.map {
      case (k, v) => k.toName.value -> SchemaChange.Init // FIXME
    })

    def toProject: Project = grouped.value.foldLeft(Project.EmptyArr) {
      case (p, (f, v)) => p.set(f, -\/ (v))
    }

    def empty = copy(grouped = Grouped(ListMap()))

    def getAll: List[(BsonField.Leaf, GroupOp)] = grouped.value.toList

    def set(field: BsonField.Leaf, value: GroupOp): Group = {
      copy(grouped = Grouped(grouped.value + (field -> value)))
    }

    def deleteAll(fields: List[BsonField.Leaf]): Group = {
      empty.setAll(getAll.filterNot(t => fields.exists(t._1 == _)))
    }

    def setAll(vs: Seq[(BsonField.Leaf, GroupOp)]) = copy(grouped = Grouped(ListMap(vs: _*)))

    def get(ref: DocVar): Option[ExprOp \/ Reshape] = ref match {
      case DocVar(_, Some(name)) => name.flatten match {
        case x :: Nil => grouped.value.get(x).map(-\/ apply)
        case _ => None
      }

      case _ => Some(\/- (Reshape.Doc(grouped.value.map { case (leaf, expr) => leaf.toName -> -\/ (expr) })))
    }

    def rhs = {
      val Bson.Doc(m) = grouped.bson

      Bson.Doc(m + ("_id" -> by.fold(_.bson, _.bson)))
    }
  }
  case class Sort(value: NonEmptyList[(BsonField, SortType)]) extends SimpleOp("$sort") with ShapePreservingOp {
    // Note: ListMap preserves the order of entries.
    def rhs = Bson.Doc(ListMap((value.map { case (k, t) => k.asText -> t.bson }).list: _*))
    
    override def toString = "Sort(NonEmptyList(" + value.map(t => t._1.toString + " -> " + t._2).list.mkString(", ") + "))"
  }
  case class Out(collection: Collection) extends SimpleOp("$out") with ShapePreservingOp {
    def rhs = Bson.Text(collection.name)
  }
  case class GeoNear(near: (Double, Double), distanceField: BsonField, 
                     limit: Option[Int], maxDistance: Option[Double],
                     query: Option[FindQuery], spherical: Option[Boolean],
                     distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
                     uniqueDocs: Option[Boolean]) extends SimpleOp("$geoNear") {
    def schema = SchemaChange.Init // FIXME

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
    ).flatten.toListMap)
  }
}
