package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

sealed trait SchemaChange {
  import SchemaChange._
  import ExprOp.DocVar
  import PipelineOp._

  def nestedField: Option[String] = this match {
    case MakeObject(fields) if fields.size == 1 => fields.headOption.map(_._1)
    case _ => None
  }

  def isNestedField: Boolean = !nestedField.isEmpty

  def nestedArray: Option[Int] = this match {
    case MakeArray(elements) if elements.size == 1 => elements.headOption.map(_._1)
    case _ => None
  }

  def isNestedArray: Boolean = !nestedArray.isEmpty

  def makeObject(name: String): SchemaChange = SchemaChange.makeObject(name -> this)

  def makeArray(index: Int): SchemaChange = SchemaChange.makeArray(index -> this)

  def replicate: Option[DocVar \/ Project] = toProject.map(_.map(_.id))

  def get(field: BsonField): Option[ExprOp \/ Reshape] = {
    toProject.flatMap(_.fold(d => Some(-\/ (d \ field)), p => p.get(DocVar.ROOT(field))))
  }

  def toProject: Option[DocVar \/ Project] = {
    val createProj = (e: ExprOp) => ((field: BsonField.Name) => Project(Reshape.Doc(Map(field -> -\/ (e)))))

    def recurseProject(f1: BsonField, s: SchemaChange): Option[DocVar \/ Reshape] = {
      for {
        either <- loop(s)
        rez    <- either.fold(
                    expr    =>  None,
                    reshape =>  (reshape \ f1).map(_.fold(
                                  e => -\/ (e.asInstanceOf[DocVar]), // Safe since it's the only expr we can create
                                  \/- apply
                                ))
                  )
      } yield rez
    }

    def loop(v: SchemaChange): Option[DocVar \/ Reshape] = v match {
      case Init => Some(-\/ (DocVar.ROOT()))

      case FieldProject(s, f) =>
        recurseProject(BsonField.Name(f), s)

      case IndexProject(s, f) =>
        recurseProject(BsonField.Index(f), s)

      case MakeObject(fs) =>
        type MapString[X] = Map[String, X]

        for {
          fs  <-  Traverse[MapString].sequence(fs.mapValues(loop _))
        } yield {
          \/- (Reshape.Doc((fs.map {
            case (name, either) =>
              BsonField.Name(name) -> either.fold(
                expr    => (-\/  (expr)),
                reshape => ( \/- (reshape))
              )
          }).toMap))
        }

      case MakeArray(es) =>
        type MapInt[X] = Map[Int, X]

        for {
          fs  <-  Traverse[MapInt].sequence(es.mapValues(loop _))
        } yield {
          \/- (Reshape.Arr((fs.map {
            case (index, either) =>
              BsonField.Index(index) -> either.fold(
                expr    => (-\/  (expr)),
                reshape => ( \/- (reshape))
              )
          }).toMap))
        }
    }

    loop(this).map(_.map(Project.apply))
  }

  def projectField(name: String): SchemaChange = FieldProject(this, name)

  def projectIndex(index: Int): SchemaChange = IndexProject(this, index)

  def simplify: SchemaChange = {
    def simplify0(v: SchemaChange): Option[SchemaChange] = v match {
      case Init => None

      case FieldProject(MakeObject(m), field) if (m.contains(field)) => 
        val child = m(field)

        Some(simplify0(child).flatMap(simplify0 _).getOrElse(child))

      case IndexProject(MakeArray(m), index) if (m.contains(index)) => 
        val child = m(index)

        Some(simplify0(child).flatMap(simplify0 _).getOrElse(child))

      case FieldProject(s, field) => simplify0(s).map(FieldProject(_, field))
      case IndexProject(s, index) => simplify0(s).map(IndexProject(_, index))

      case MakeObject(m) => 
        type MapString[X] = Map[String, X]

        Traverse[MapString].sequence(m.mapValues(simplify0 _)).map(MakeObject.apply)

      case MakeArray(m)  => 
        type MapInt[X] = Map[Int, X]

        Traverse[MapInt].sequence(m.mapValues(simplify0 _)).map(MakeArray.apply)        
    }

    simplify0(this).getOrElse(this)
  }

  def isObject = this.simplify match {
    case MakeObject(_) => true
    case _ => false 
  }

  def isArray = this.simplify match {
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

    case (MakeArray(m1), MakeArray(m2)) => m2.forall(t2 => m1.exists(t1 => t1._2.subsumes(t2._2))) // ???

    case (FieldProject(s1, v1), FieldProject(s2, v2)) => v1 == v2 && s1.subsumes(s2)

    case (IndexProject(s1, v1), IndexProject(s2, v2)) => v1 == v2 && s1.subsumes(s2)

    case _ => false
  }

  def rebase(base: SchemaChange): SchemaChange = this match {
    case Init               => base
    case FieldProject(s, f) => FieldProject(s.rebase(base), f)
    case IndexProject(s, f) => IndexProject(s.rebase(base), f)
    case MakeObject(fs)     => MakeObject(fs.mapValues(_.rebase(base)))
    case MakeArray(es)      => MakeArray(es.mapValues(_.rebase(base)))
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
        (es.map {
          case (index, schema) => 
            val ni = BsonField.Index(index)

            patchField0(schema, f).map(_.fold(_ => \/- (ni), f => \/- (ni \ f)))

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
        (es.map {
          case (name, schema) => 
            val ni = BsonField.Index(name)

            patchField0(schema, ni)

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

  def makeArray(elements: (Int, SchemaChange)*): SchemaChange = MakeArray(elements.toMap)

  case object Init extends SchemaChange

  final case class FieldProject(source: SchemaChange, name: String) extends SchemaChange
  final case class IndexProject(source: SchemaChange, index: Int) extends SchemaChange

  final case class MakeObject(fields: Map[String, SchemaChange]) extends SchemaChange
  final case class MakeArray(elements: Map[Int, SchemaChange]) extends SchemaChange
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
