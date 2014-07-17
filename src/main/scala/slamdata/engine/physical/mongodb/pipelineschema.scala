package slamdata.engine.physical.mongodb

// import slamdata.engine.Error

// import com.mongodb.DBObject

import scalaz._
import Scalaz._

// import slamdata.engine.{RenderTree, Terminal, NonTerminal}
// import slamdata.engine.fp._
/*


  |             |
  |             |
  |             |
  |             |
  |             |
  |             |
  \             /
   \           /
    \         /
     \       / 
      \     /
       \   /
        \_/
        (_)  <----- Point of unification
         |
         |
         |
         |
         |
         |

*/
sealed trait SchemaChange {
  import SchemaChange._
  import ExprOp.DocVar
  import PipelineOp._

  def toProject: Option[(BsonField.Name => Project) \/ Project] = {
    import Either3._

    val createProj = (e: ExprOp) => ((field: BsonField.Name) => Project(Reshape.Doc(Map(field -> -\/ (e)))))

    def recurseProject(f1: BsonField, s: SchemaChange): Option[Either3[DocVar, ExprOp, Reshape]] = {
      for {
        either <- loop(s)
        rez    <- either.fold(
                    f2      =>  Some(left3(f2 \ f1)), 
                    expr    =>  None,
                    reshape =>  (reshape \ f1).map(_.fold(
                                  {
                                    case d : DocVar => left3(d)
                                    case e : ExprOp => middle3(e)
                                  },
                                  reshape => right3(reshape)
                                ))
                  )
      } yield rez
    }

    def loop(v: SchemaChange): Option[Either3[DocVar, ExprOp, Reshape]] = v match {
      case Init => Some(left3(DocVar.ROOT()))

      case FieldProject(s, f) =>
        recurseProject(BsonField.Name(f), s)

      case IndexProject(s, f) =>
        recurseProject(BsonField.Index(f), s)

      case MakeObject(fs) =>
        type MapString[X] = Map[String, X]

        for {
          fs  <-  Traverse[MapString].sequence(fs.mapValues(loop _))
        } yield {
          right3(Reshape.Doc((fs.map {
            case (name, either) =>
              BsonField.Name(name) -> either.fold(
                f2      => (-\/  (f2)),
                expr    => (-\/  (expr)),
                reshape => ( \/- (reshape))
              )
          }).toMap))
        }

      case MakeArray(es) =>
        for {
          es <- es.map(loop _).sequenceU
        } yield {
          right3(Reshape.Arr((es.zipWithIndex.map {
            case (either, index) =>
              BsonField.Index(index) -> either.fold(
                f2      => (-\/  (f2)),
                expr    => (-\/  (expr)),
                reshape => ( \/- (reshape))
              )
          }).toMap))
        }
    }

    loop(this).map(_.fold(
      ref     => -\/  (createProj(ref)),
      expr    => -\/  (createProj(expr)),
      reshape =>  \/- (Project(reshape))
    ))
  }

  def projectField(name: String): SchemaChange = FieldProject(this, name)

  def projectIndex(index: Int): SchemaChange = IndexProject(this, index)

  def isObject = this match {
    case MakeObject(_) => true
    case _ => false 
  }

  def isArray = this match {
    case MakeArray(_) => true
    case _ => false 
  }

  def concat(that: SchemaChange): Option[SchemaChange] = (this, that) match {
    case (MakeObject(m1), MakeObject(m2)) => Some(MakeObject(m1 ++ m2))
    case (MakeArray(m1), MakeArray(m2)) => Some(MakeArray(m1 ++ m2))
    case _ => None
  }

  def subsumes(that: SchemaChange): Boolean = (this, that) match {
    case (x, y) if x == y => true

    case (MakeObject(m1), MakeObject(m2)) => m2.forall(t2 => m1.exists(t1 => t2._1 == t1._1 && t1._2.subsumes(t2._2)))

    case (MakeArray(m1_), MakeArray(m2_)) =>
      val m1 = m1_.zipWithIndex
      val m2 = m2_.zipWithIndex

      m2.forall(t2 => m1.exists(t1 => t1._1.subsumes(t2._1)))

    case _ => false
  }

  def rebase(base: SchemaChange): SchemaChange = this match {
    case Init               => base
    case FieldProject(s, f) => FieldProject(s.rebase(base), f)
    case IndexProject(s, f) => IndexProject(s.rebase(base), f)
    case MakeObject(fs)     => MakeObject(fs.mapValues(_.rebase(base)))
    case MakeArray(es)      => MakeArray(es.map(_.rebase(base)))
  }

  def patchField(base: SchemaChange): BsonField => Option[BsonField.Root \/ BsonField] = f => patch(base)(\/- (f))

  def patchRoot(base: SchemaChange): Option[BsonField.Root \/ BsonField] = patch(base)(-\/ (BsonField.Root))

  def patch(base: SchemaChange): (BsonField.Root \/ BsonField) => Option[BsonField.Root \/ BsonField] = { e =>
    def patchField0(v: SchemaChange, f: BsonField): Option[BsonField.Root \/ BsonField] = v match {
      case x if (x.subsumes(base)) => Some(\/- (f))

      case Init => None

      case MakeObject(fs) =>
        (fs.map {
          case (name, schema) => 
            val nf = BsonField.Name(name)

            patchField0(schema, f).map(_.fold(_ => \/- (nf), f => \/- (nf \ f)))

        }).collect { case Some(x) => x }.headOption

      case MakeArray(es) =>
        (es.zipWithIndex.map {
          case (schema, index) => 
            val nf = BsonField.Index(index)

            patchField0(schema, f).map(_.fold(_ => \/- (nf), f => \/- (nf \ f)))
        }).collect { case Some(x) => x }.headOption

      case FieldProject(s, n) => 
        f.flatten match {
          case BsonField.Name(`n`) :: xs => Some(BsonField(xs).map(\/- apply).getOrElse(-\/ (BsonField.Root)))
          case _ => None 
        }

      case IndexProject(s, i) =>
        f.flatten match {
          case BsonField.Index(`i`) :: xs => Some(BsonField(xs).map(\/- apply).getOrElse(-\/ (BsonField.Root)))
          case _ => None
        }
    }

    def patchRoot0(v: SchemaChange): Option[BsonField.Root \/ BsonField] = v match {
      case x if (x.subsumes(base)) => Some(-\/ (BsonField.Root))

      case Init => None

      case MakeObject(fs) =>
        (fs.map {
          case (name, schema) => 
            val nf = BsonField.Name(name)

            patchField0(schema, nf)

        }).collect { case Some(x) => x }.headOption

      case MakeArray(es) =>
        (es.zipWithIndex.map {
          case (schema, index) => 
            val nf = BsonField.Index(index)

            patchField0(schema, nf)
        }).collect { case Some(x) => x }.headOption

      case _ => None
    }

    e.fold(
      root  => patchRoot0(this),
      field => patchField0(this, field)
    )
  }
}
object SchemaChange {
  def makeObject(fields: (String, SchemaChange)*): SchemaChange = MakeObject(fields.toMap)

  def makeArray(elements: SchemaChange*): SchemaChange = MakeArray(elements.toList)

  case object Init extends SchemaChange

  case class FieldProject(source: SchemaChange, name: String) extends SchemaChange
  case class IndexProject(source: SchemaChange, index: Int) extends SchemaChange

  case class MakeObject(fields: Map[String, SchemaChange]) extends SchemaChange
  case class MakeArray(elements: List[SchemaChange]) extends SchemaChange
}

sealed trait PipelineSchema {
  import PipelineSchema._

  def accum(op: PipelineOp): PipelineSchema = op match {
    case (p @ PipelineOp.Project(_)) => p.schema
    case (g @ PipelineOp.Group(_, _)) => g.schema
    case _ => this
  }

  def field(name: String): Option[PipelineSchema] = this match {
    case Init => Some(Succ(Map(BsonField.Name(name) -> \/- (Init))))
    case Succ(m) => m.get(BsonField.Name(name)).map(_.fold(_ => None, Some.apply)).getOrElse(None)
  }

  def index(index: Int): Option[PipelineSchema] = this match {
    case Init => Some(Succ(Map(BsonField.Index(index) -> \/- (Init))))
    case Succ(m) => m.get(BsonField.Index(index)).map(_.fold(_ => None, Some.apply)).getOrElse(None)
  }
}
object PipelineSchema {
  import ExprOp.DocVar
  import PipelineOp.{Reshape, Project}

  def apply(ops: List[PipelineOp]): PipelineSchema = ops.foldLeft[PipelineSchema](Init)((s, o) => s.accum(o))

  case object Init extends PipelineSchema
  case class Succ(proj: Map[BsonField.Leaf, Unit \/ PipelineSchema]) extends PipelineSchema {
    private def toProject0(prefix: DocVar, s: Succ): Project = {
      def rec[A <: BsonField.Leaf](prefix: DocVar, x: A, y: Unit \/ PipelineSchema): (A, ExprOp \/ Reshape) = {
        x -> y.fold(
          _ => -\/ (prefix), 
          {
            case Init => -\/ (ExprOp.DocVar.ROOT())
            case s @ Succ(proj) => \/- (s.toProject0(prefix, s).shape)
          }
        )
      }

      val indices = proj.collect {
        case (x : BsonField.Index, y) => 
          rec(prefix \ x, x, y)
      }

      val fields = proj.collect {
        case (x : BsonField.Name, y) =>
          rec(prefix \ x, x, y)
      }

      val arr = Reshape.Arr(indices)
      val doc = Reshape.Doc(fields)

      Project(arr ++ doc)
    }

    def toProject: Project = toProject0(DocVar.ROOT(), this)
  }
}
