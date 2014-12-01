package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine._
import Workflow._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.std.StdLib._
import slamdata.engine.javascript._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

sealed trait WorkflowBuilderError extends Error
object WorkflowBuilderError {
  case object CannotObjectConcatExpr extends WorkflowBuilderError {
    def message = "Cannot object concat an expression"
  }
  case object InvalidSortBy extends WorkflowBuilderError {
    def message = "The sort by set has an invalid structure"
  }
  case class InvalidOperation(operation: String, msg: String)
      extends WorkflowBuilderError {
    def message = "Can not perform `" + operation + "`, because " + msg
  }
  case class UnsupportedDistinct(message: String) extends WorkflowBuilderError
  case class UnsupportedJoinCondition(func: Mapping) extends WorkflowBuilderError {
    def message = "Joining with " + func.name + " is not currently supported"
  }
}

sealed trait WorkflowBuilderF[+A]

/**
 * A `WorkflowBuilder` consists of a graph of operations, a structure, and a
 * base mod for that structure.
 */
object WorkflowBuilder {
  import Workflow._
  import ExprOp._
  import IdHandling._

  type WorkflowBuilder = Term[WorkflowBuilderF]

  type Expr = ExprOp \/ JsMacro

  case class CollectionBuilderF(
    graph: Workflow,
    base: DocVar,
    struct: SchemaChange) extends WorkflowBuilderF[Nothing]
  object CollectionBuilder {
    def apply(graph: Workflow, base: DocVar, struct: SchemaChange) =
      Term[WorkflowBuilderF](new CollectionBuilderF(graph, base, struct))
  }
  case class ValueBuilderF(value: Bson) extends WorkflowBuilderF[Nothing]
  object ValueBuilder {
    def apply(value: Bson) = Term[WorkflowBuilderF](new ValueBuilderF(value))
  }
  case class ExprBuilderF[A](src: A, expr: Expr) extends WorkflowBuilderF[A]
  object ExprBuilder {
    def apply(src: WorkflowBuilder, expr: Expr) =
      Term[WorkflowBuilderF](new ExprBuilderF(src, expr))
  }
  
  // NB: The shape is more restrictive than $project because we may need to
  //     convert it to a GroupBuilder, and a nested Reshape can be realized with
  //     a chain of DocBuilders, leaving the collapsing to Workflow.coalesce.
  case class DocBuilderF[A](src: A, shape: ListMap[BsonField.Name, Expr])
      extends WorkflowBuilderF[A]
  object DocBuilder {
    def apply(src: WorkflowBuilder, shape: ListMap[BsonField.Name, Expr]) =
      Term[WorkflowBuilderF](new DocBuilderF(src, shape))
  }
  case class GroupBuilderF[A](src: A, key: ExprOp \/ Reshape, contents: GroupContents, id: GroupId)
      extends WorkflowBuilderF[A]
  object GroupBuilder {
    def apply(src: WorkflowBuilder, key: ExprOp \/ Reshape, contents: GroupContents, id: GroupId) =
      Term[WorkflowBuilderF](new GroupBuilderF(src, key, contents, id))
  }

  sealed trait GroupContents
  object GroupContents {
    case class Field(content: DocVar) extends GroupContents
    case class Pushed(content: ExprOp) extends GroupContents
    case class Reduced(content: GroupOp) extends GroupContents
    case class Document(content: ListMap[BsonField.Name, ExprOp \/ GroupOp])
      extends GroupContents
  }
  import GroupContents._

  case class GroupId(srcs: List[WorkflowBuilder]) {
    override def toString = hashCode.toHexString
  }

  private def rewriteObjRefs(
    obj: ListMap[BsonField.Name, ExprOp \/ GroupOp])(
    f: PartialFunction[DocVar, DocVar]) =
    obj ∘ (_.fold(
      expr => -\/(expr.rewriteRefs(f)),
      _.rewriteRefs(f) match {
        case g : GroupOp => \/-(g)
        case _ => sys.error("Transformation changed the type -- error!")
      }
    ))

  private def rewriteGroupRefs(
    contents: GroupContents)(
    f: PartialFunction[DocVar, DocVar]) =
    contents match {
      case Field(d)         => Field(f(d))
      case Pushed(expr)     => Pushed(expr.rewriteRefs(f))
      case Reduced(grouped) => grouped.rewriteRefs(f) match {
        case g : GroupOp => Reduced(g)
        case _ => sys.error("Transformation changed the type -- error!")
      }
      case Document(obj)    => Document(rewriteObjRefs(obj)(f))
    }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: DocVar) =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: DocVar): Expr =
    expr.bimap(
      _.rewriteRefs(prefixBase(base)),
      js => base.toJs >>> js)
    
  def rewritePrefix(wb: WorkflowBuilder, base: DocVar) = 
    wb.unFix match {
      case CollectionBuilderF(_, _, _) => wb
      case ValueBuilderF(_)            => wb
      case ExprBuilderF(src, expr)     => ExprBuilder(src, rewriteExprPrefix(expr, base))
      case DocBuilderF(src, doc)       => DocBuilder(src, rewriteDocPrefix(doc, base))
      case GroupBuilderF(src, key, contents, id) =>
        val f = prefixBase(base)
        GroupBuilder(src,
          key.bimap(_.rewriteRefs(f), _.rewriteRefs(f)),
          rewriteGroupRefs(contents)(f),
          id)
    }

  def chainBuilder(wb: WorkflowBuilder, base: DocVar, struct: SchemaChange)(f: WorkflowOp) =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(g, b, _) =>
        CollectionBuilder(
          Term[WorkflowF](Workflow.rewriteRefs(f(g).unFix, prefixBase(b))),
          base,
          struct)
    })

  type EitherE[X] = Error \/ X
  type M[X] = StateT[EitherE, NameGen, X]
  type MId[X] = State[NameGen, X]

  // Wrappers for results that don't use state:
  def emit[A](a: A): M[A] = lift(\/-(a))
  def fail[A](e: Error): M[A] = lift(-\/(e))
  def lift[A](v: Error \/ A): M[A] =
    StateT[EitherE, NameGen, A](s => v.map(s -> _))

  // Wrappers for results that don't fail:
  def emitSt[A](v: State[NameGen, A]): M[A] =
    StateT[EitherE, NameGen, A](s => \/-(v.run(s)))

  def swapM[A](v: State[NameGen, Error \/ A]): M[A] =
    StateT[EitherE, NameGen, A](s => { val (s1, x) = v.run(s); x.map(s1 -> _) })

  def commonMap[K, A, B](m: ListMap[K, A \/ B])(f: A => Error \/ B): Error \/ (ListMap[K, A] \/ ListMap[K, B]) = {
    val allAs = (m ∘ (_.swap.toOption)).sequenceU
    allAs.map(l => \/-(-\/(l))).getOrElse((m ∘ (_.fold(f, \/.right))).sequenceU.map(\/.right))
  }

  private def commonShape(shape: ListMap[BsonField.Name, Expr]) = commonMap(shape)(ExprOp.toJs)
        
  private def toCollectionBuilder(wb: WorkflowBuilder): M[CollectionBuilderF] =
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => emit(cb)
      case ValueBuilderF(value) =>
        emit(CollectionBuilderF($pure(value), DocVar.ROOT(), SchemaChange.Init))
      case ExprBuilderF(src, -\/(d @ DocVar(_, _))) =>
        toCollectionBuilder(src).map(_ match {
          case CollectionBuilderF(graph, base, _) =>
            CollectionBuilderF(graph, base \\ d, SchemaChange.Init)
        })
      case ExprBuilderF(src, expr) => for {
        cb <- toCollectionBuilder(src)
        name <- emitSt(freshName)
      } yield cb match {
        case CollectionBuilderF(graph, base, struct) =>
          CollectionBuilderF(
            chain(graph,
              rewriteExprPrefix(expr, base).fold(
                op => $project(Reshape.Doc(ListMap(name -> -\/(op)))),
                js => $simpleMap(JsMacro(x => JsCore.Obj(ListMap(name.asText -> js(x))).fix)))),
            DocField(name),
            struct)
      }
      case DocBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          commonShape(rewriteDocPrefix(shape, base)).fold(
            fail(_),
            s => emit(CollectionBuilderF(
              chain(wf,
                s.fold(
                  exprOps => $project(Reshape.Doc(exprOps ∘ \/.left)),
                  jsExprs => $simpleMap(JsMacro(x => Term(JsCore.Obj(
                    jsExprs.map { case (name, expr) => name.asText -> expr(x) }
                  )))))),
              DocVar.ROOT(),
              SchemaChange.MakeObject(shape.map {
                case (k, _) => k.asText -> SchemaChange.Init
              }))))
        }
      case GroupBuilderF(src, _, Field(doc), _) =>
        // NB: This case just winds up a single value, then unwinds it. It’s
        //     effectively a no-op, so we just use the src and expr
        toCollectionBuilder(ExprBuilder(src, -\/(doc)))
      case GroupBuilderF(src, _, Pushed(expr), _) =>
        // NB: This case just winds up a single value, then unwinds it. It’s
        //     effectively a no-op, so we just use the src and expr
        toCollectionBuilder(ExprBuilder(src, -\/(expr)))
      case GroupBuilderF(src, key, Reduced(grouped), _) =>
        for {
          cb <- toCollectionBuilder(src)
          rootName <- emitSt(freshName)
        } yield cb match {
          case CollectionBuilderF(wf, base, struct) =>
          CollectionBuilderF(
            chain(wf,
              $group(Grouped(ListMap(rootName -> grouped)).rewriteRefs(prefixBase(base)),
                key.bimap(_.rewriteRefs (prefixBase(base)), _.rewriteRefs(prefixBase(base))))),
            DocField(rootName),
            struct)
        }
      case GroupBuilderF(src, key, Document(obj), _) =>
        val (ungrouped, grouped) =
          obj.foldLeft[(ListMap[BsonField.Name, ExprOp], ListMap[BsonField.Leaf, GroupOp])]((ListMap.empty[BsonField.Name, ExprOp], ListMap.empty[BsonField.Leaf, GroupOp]))((acc, item) =>
            item match {
              case (k, -\/(v)) =>
                ((x: ListMap[BsonField.Name, ExprOp]) => x + (k -> v)).first(acc)
              case (k, \/-(v)) =>
                ((x: ListMap[BsonField.Leaf, GroupOp]) => x + (k -> v)).second(acc)
            })

        workflow(src).flatMap(_ match {
          case (wf, base) =>
            emitSt(ungrouped.size match {
              case 0 =>
                state[NameGen, Workflow](chain(wf,
                  $group(Grouped(grouped).rewriteRefs(prefixBase(base)),
                    key.bimap(_.rewriteRefs(prefixBase(base)), _.rewriteRefs(prefixBase(base))))))
              case 1 =>
                state[NameGen, Workflow](chain(wf,
                  $group(Grouped(
                    obj.transform {
                      case (_, -\/(v)) => Push(v)
                      case (_, \/-(v)) => v.rewriteRefs(prefixBase(base))
                    }),
                    key.bimap(_.rewriteRefs(prefixBase(base)), _.rewriteRefs(prefixBase(base)))),
                                $unwind(DocField(ungrouped.head._1))))
              case _ => for {
                ungroupedName <- freshName
                groupedName <- freshName
              } yield
                chain(wf,
                  $project(Reshape.Doc(ListMap(
                    ungroupedName -> \/-(Reshape.Doc(ungrouped.map {
                      case (k, v) => k -> -\/(v.rewriteRefs(prefixBase(base)))
                    })),
                    groupedName -> -\/(DocVar.ROOT())))),
                  $group(Grouped(
                      (grouped ∘ (_.rewriteRefs(prefixBase(DocField(groupedName))))) + 
                        (ungroupedName -> Push(DocField(ungroupedName)))),
                    key.bimap(_.rewriteRefs(prefixBase(DocField(groupedName))), _.rewriteRefs(prefixBase(DocField(groupedName))))),
                  $unwind(DocField(ungroupedName)),
                  $project(Reshape.Doc(obj.transform {
                    case (k, -\/(_)) => -\/(DocField(ungroupedName \ k))
                    case (k, \/-(_)) => -\/(DocField(k))
                  })))
            }).map(CollectionBuilderF(
              _,
              DocVar.ROOT(),
              SchemaChange.MakeObject(obj.map {
                case (k, _) => (k.asText -> SchemaChange.Init)
              })))
        })
    }

  def workflow(wb: WorkflowBuilder): M[(Workflow, DocVar)] =
    toCollectionBuilder(wb).map(x => (x.graph, x.base))

  def build(wb: WorkflowBuilder): M[Workflow] =
    toCollectionBuilder(wb).flatMap(_ match {
      case CollectionBuilderF(graph, base, struct) => base match {
        case DocVar.ROOT(None) => emit(finish(graph))
        case base =>
          val g1 = struct.shift(base).map(t => chain(graph, Workflow.$project(t._1, t._2))).getOrElse(graph)
          build(CollectionBuilder(g1, DocVar.ROOT(), struct))
      }
    })

  private def $project(shape: Reshape): WorkflowOp =
    Workflow.$project(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(Function.const(IncludeId)))

  def asLiteral(wb: WorkflowBuilder) =
    asExprOp(wb).collect { case (x @ Literal(_)) => x }

  private def foldBuilders(src: WorkflowBuilder, others: List[WorkflowBuilder]) =
    others.foldLeftM[M, (WorkflowBuilder, DocVar, List[DocVar])](
      (src, DocVar.ROOT(), Nil)) {
      case ((wf, base, fields), x) =>
        merge(wf, x) { (lbase, rbase, src) =>
          emit((src, lbase \\ base, fields.map(lbase \\ _) :+ rbase))
        }
    }

  def filter(src: WorkflowBuilder, those: List[WorkflowBuilder], sel: PartialFunction[List[BsonField], Selector]):
      M[WorkflowBuilder] =
    foldBuilders(src, those).flatMap { case (wb, base, fields) =>
      fields.map(_.deref).sequence.fold(
        fail[WorkflowBuilder](WorkflowBuilderError.CannotObjectConcatExpr))(
        sel.lift(_).fold(
          fail[WorkflowBuilder](WorkflowBuilderError.CannotObjectConcatExpr))(
          s => chainBuilder(wb, base, SchemaChange.Init)($match(s))))
    }

  def expr1(wb: WorkflowBuilder)(f: ExprOp => ExprOp): M[WorkflowBuilder] =
    wb.unFix match {
      case GroupBuilderF(wb0, key, Field(d), id) => emit(GroupBuilder(wb0, key, Pushed(f(d)), id))
      case ExprBuilderF(wb0, -\/ (expr1))    => emit(ExprBuilder(wb0, -\/(f(expr1))))
      case ExprBuilderF(wb0,  \/-(js1))      => lift(toJs(f(DocVar.ROOT())).map(js => ExprBuilder(wb0, \/-(js1 >>> js))))
      case _ => emit(ExprBuilder(wb, -\/(f(DocVar.ROOT()))))
    }

  def expr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder)(f: (ExprOp, ExprOp) => ExprOp):
    M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(bson)) => expr1(wb1)(f(_, Literal(bson)))
      case (ValueBuilderF(bson), _) => expr1(wb2)(f(Literal(bson), _))
      case _ =>
        merge(wb1, wb2) { (lbase, rbase, src) =>
          emit(ExprBuilder(src, -\/(f(lbase, rbase))))
        }
    }
        
  def expr3(wb1: WorkflowBuilder, wb2: WorkflowBuilder, wb3: WorkflowBuilder)
            (f: (ExprOp, ExprOp, ExprOp) => ExprOp): M[WorkflowBuilder] = {
    def nest(lname: BsonField.Name, rname: BsonField.Name) =
      (lbase: DocVar, rbase: DocVar, src: WorkflowBuilder) =>
        emit(DocBuilder(src, ListMap(lname -> -\/(lbase), rname -> -\/(rbase))))

    for {
      l      <- emitSt(freshName)
      ll     <- emitSt(freshName)
      lr     <- emitSt(freshName)
      r      <- emitSt(freshName)
      p12    <- merge(wb1, wb2)(nest(ll, lr))
      p123   <- merge(p12, wb3)(nest(l, r))
    } yield
      ExprBuilder(p123, -\/(f(DocField(l \ ll), DocField(l \ lr), DocField(r))))
  }

  def jsExpr1(wb: WorkflowBuilder, js: JsMacro): M[WorkflowBuilder] =
    wb.unFix match {
      case ExprBuilderF(wb1, -\/ (expr1))        => lift(toJs(expr1).map(js1 => ExprBuilder(wb1, \/-(js1 >>> js))))
      case ExprBuilderF(wb1,  \/-(js1))          => emit(ExprBuilder(wb1, \/-(js1 >>> js)))
      case GroupBuilderF(wb1, key, Field(d), id) => emit(GroupBuilder(ExprBuilder(wb1, \/-(d.toJs >>> js)), key, Field(DocVar.ROOT()), id))
      case _                                     => emit(ExprBuilder(wb, \/-(js)))
    }

  def jsExpr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder, js: (Term[JsCore], Term[JsCore]) => Term[JsCore]): M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(JsCore(lit))) => jsExpr1(wb1, JsMacro(x => js(x, lit)))
      case (ValueBuilderF(JsCore(lit)), _) => jsExpr1(wb2, JsMacro(x => js(lit, x)))
      
      case _ =>
        merge(wb1, wb2) { (lbase, rbase, src) =>
          emit(ExprBuilder(src, \/-(JsMacro(x => js(lbase.toJs(x), rbase.toJs(x))))))
        }
    }

  def makeObject(wb: WorkflowBuilder, name: String): WorkflowBuilder =
    wb.unFix match {
      case ValueBuilderF(value) =>
        ValueBuilder(Bson.Doc(ListMap(name -> value)))
      case GroupBuilderF(src, key, Field(d), id) =>
        GroupBuilder(src, key,
          Document(ListMap(BsonField.Name(name) -> -\/(d))),
          id)
      case GroupBuilderF(src, key, Pushed(expr), id) =>
        GroupBuilder(src, key,
          Document(ListMap(BsonField.Name(name) -> -\/(expr))),
          id)
      case GroupBuilderF(src, key, Reduced(grouped), id) =>
        GroupBuilder(src, key,
          Document(ListMap(BsonField.Name(name) -> \/-(grouped))),
          id)
      case ExprBuilderF(src, expr) =>
        DocBuilder(src, ListMap(BsonField.Name(name) -> expr))
      case _ =>
        DocBuilder(wb, ListMap(BsonField.Name(name) -> -\/(DocVar.ROOT())))
    }

  def makeArray(wb: WorkflowBuilder): M[WorkflowBuilder] = wb.unFix match {
    case ValueBuilderF(value) => emit(ValueBuilder(Bson.Arr(List(value))))
    case _ =>
      toCollectionBuilder(wb).map {
        case CollectionBuilderF(graph, base, struct) =>
          CollectionBuilder(
            chain(graph,
              $project(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/(base))))),
            DocVar.ROOT(),
            struct.makeArray(0))
      }
  }

  // TODO: handle concating value, expr, or collection with group (#439)
  def objectConcat(wb1: WorkflowBuilder, wb2: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def delegate = objectConcat(wb2, wb1)
    def mergeGroups(s1: WorkflowBuilder, s2: WorkflowBuilder, c1: GroupContents, c2: GroupContents, k1: ExprOp \/ Reshape, id1: GroupId):
      M[((DocVar, DocVar), WorkflowBuilder)] =
      merge(s1, s2) { case (lbase, rbase, src) =>
        val key = k1.bimap(_.rewriteRefs(prefixBase(lbase)), _.rewriteRefs(prefixBase(lbase)))
        def newShape(c: GroupContents, base: DocVar): MId[(DocVar, ListMap[BsonField.Name, ExprOp \/ GroupOp])] = c match {
          case Document(l) => state(DocVar.ROOT() -> rewriteObjRefs(l)(prefixBase(base)))
          case Reduced(x)  => freshName.map(name => DocField(name) -> ListMap(name ->  \/-(x.rewriteRefs(prefixBase(base)))))
          case Pushed(x)   => freshName.map(name => DocField(name) -> ListMap(name -> -\/ (x.rewriteRefs(prefixBase(base)))))
          case Field(d)    => freshName.map(name => DocField(name) -> ListMap(name -> -\/ (d.rewriteRefs(prefixBase(base)))))
        }
        emitSt((newShape(c1, lbase) |@| newShape(c2, rbase)) { case ((lb, lshape), (rb, rshape)) =>
          (lb, rb) -> GroupBuilder(src, key, Document(lshape ++ rshape), id1)  // FIXME: possible shadowing
        })
      }

    def generalMerge =
      emitSt(freshId).flatMap { name =>
        def builderWithUnknowns(
          src: WorkflowBuilder,
          fields: List[Js.Expr => Js.Stmt]) =
          chainBuilder(src, DocVar.ROOT(), SchemaChange.Init)(
            $map($Map.mapMap(name,
              Js.Call(
                Js.AnonFunDecl(List("rez"),
                  fields.map(_(Js.Ident("rez"))) :+ Js.Return(Js.Ident("rez"))),
                List(Js.AnonObjDecl(Nil))))))

        def side(wb: WorkflowBuilder, base: DocVar):
            Error \/ ((Js.Expr => Js.Stmt) \/ List[BsonField.Name])=
          wb.unFix match {
            case CollectionBuilderF(_, _, _) =>
              \/-(-\/($Reduce.copyAllFields(base.toJs(JsCore.Ident(name).fix).toJs)))
            case ValueBuilderF(Bson.Doc(map)) =>
              \/-(\/-(map.keys.map(BsonField.Name(_)).toList))
            case DocBuilderF(_, shape) => \/-(\/-(shape.keys.toList))
            // TODO: Restrict to DocVar, Literal, Let, Cond, and IfNull (see #471)
            case ExprBuilderF(_, _) =>
              \/-(-\/($Reduce.copyAllFields(base.toJs(JsCore.Ident(name).fix).toJs)))
            case _ =>
              -\/(WorkflowBuilderError.InvalidOperation(
                "objectConcat",
                "values are not both documents: " + wb))
          }

        merge(wb1, wb2) { (left, right, list) =>
          lift((side(wb1, left) |@| side(wb2, right))((_, _) match {
            case (\/-(f1), \/-(f2)) =>
              if ((f1 intersect f2).isEmpty)
                emit(DocBuilder(list,
                  (f1.map(k => k -> -\/(left \ k)) ++
                    f2.map(k => k -> -\/(right \ k))).toListMap))
              else fail[WorkflowBuilder](WorkflowBuilderError.InvalidOperation(
                "objectConcat",
                "conflicting keys"))
            case (\/-(_), -\/(_)) => delegate
            case (-\/(f1), \/-(f2)) =>
              builderWithUnknowns(
                list,
                List(f1) ++
                  f2.map(k =>
                    $Reduce.copyOneField(
                      Js.Access(_, Js.Str(k.asText)),
                      (right \ k).toJs(JsCore.Ident(name).fix).toJs)))
            case (-\/(f1), -\/(f2)) =>
              builderWithUnknowns(list, List(f1, f2))
          })).join
        }
      }

    (wb1.unFix, wb2.unFix) match {
      case (ValueBuilderF(Bson.Doc(map1)), ValueBuilderF(Bson.Doc(map2))) =>
        emit(ValueBuilder(Bson.Doc(map1 ++ map2)))

      case (ValueBuilderF(Bson.Doc(map1)), DocBuilderF(s2, shape2)) 
        if (map1.keySet.map(BsonField.Name(_)) intersect shape2.keySet).isEmpty =>
          emit(DocBuilder(s2, 
            map1.map { case (k, v) => BsonField.Name(k) -> -\/(Literal(v)) } ++ 
            shape2))
      case (DocBuilderF(_, shape1), ValueBuilderF(Bson.Doc(map2)))
       if (shape1.keySet intersect map2.keySet.map(BsonField.Name(_))).isEmpty => delegate 

      case (
        GroupBuilderF(s1, k1, c1 @ Document(_), id1),
        GroupBuilderF(s2, k2, c2 @ Document(_), id2))
          if id1 == id2 =>
            mergeGroups(s1, s2, c1, c2, k1, id1).map(_._2)

      case (
        GroupBuilderF(s1, k1, c1 @ Document(_), id1),
        DocBuilderF(Term(GroupBuilderF(s2, k2, c2, id2)), shape2)) 
          if id1 == id2 =>
            mergeGroups(s1, s2, c1, c2, k1, id1).map { case ((glbase, grbase), g) =>
              val shape = c1.content.transform { case (n, _) => -\/(DocField(n)) } ++
                          (shape2 ∘ (rewriteExprPrefix(_, grbase)))
              DocBuilder(g, shape)
            }
      case (
        DocBuilderF(Term(GroupBuilderF(_, k1, _, id1)), _),
        GroupBuilderF(_, k2, Document(_), id2))
          if id1 == id2 =>
        delegate
      
      case (
        DocBuilderF(Term(GroupBuilderF(s1, k1, c1, id1)), shape1),
        DocBuilderF(Term(GroupBuilderF(s2, k2, c2, id2)), shape2))
          if id1 == id2 && (shape1.keySet intersect shape2.keySet).isEmpty =>
            mergeGroups(s1, s2, c1, c2, k1, id1).flatMap { case ((glbase, grbase), g) =>
              val shape = (shape1 ∘ (rewriteExprPrefix(_, glbase))) ++
                            (shape2 ∘ (rewriteExprPrefix(_, grbase)))
              emit(DocBuilder(g, shape))
            }

      case (
        DocBuilderF(s1 @ Term(CollectionBuilderF(_, _, _)), shape),
        g2 @ GroupBuilderF(_, -\/(Literal(Bson.Null)), _, id2)) =>
          objectConcat(
            DocBuilder(GroupBuilder(s1, -\/(Literal(Bson.Null)), Field(DocVar.ROOT()), id2), shape),
            Term(g2))
      case (
        GroupBuilderF(_, -\/(Literal(Bson.Null)), _, _),
        DocBuilderF(Term(CollectionBuilderF(_, _, _)), _)) =>
          delegate
      
      case (
        DocBuilderF(s1 @ Term(CollectionBuilderF(_, _, _)), shape),
        d2 @ DocBuilderF(Term(GroupBuilderF(_, -\/(Literal(Bson.Null)), _, id2)), _)) =>
          objectConcat(
            DocBuilder(GroupBuilder(s1, -\/(Literal(Bson.Null)), Field(DocVar.ROOT()), id2), shape),
            Term(d2))
      case (
        DocBuilderF(Term(GroupBuilderF(_, -\/(Literal(Bson.Null)), _, _)), _),
        DocBuilderF(Term(CollectionBuilderF(_, _, _)), _)) =>
          delegate
      
      case (
        DocBuilderF(s1 @ Term(CollectionBuilderF(_, _, _)), shape1),
        DocBuilderF(s2 @ Term(CollectionBuilderF(_, _, _)), shape2))
          if (shape1.keySet intersect shape2.keySet).isEmpty =>
            // NB: it's safe to merge here because the sources are both collections
            merge(s1, s2) { case (lbase, rbase, src) =>
              emit(DocBuilder(src, rewriteDocPrefix(shape1, lbase) ++ rewriteDocPrefix(shape2, rbase)))
            }

      // NB: JS-exprs cannot be rolled into $group ops, leading to this somewhat exceptional case
      case (
        GroupBuilderF(s1, k1, c1 @ Document(_), id1),
        GroupBuilderF(Term(
          ExprBuilderF(Term(
            GroupBuilderF(s2, k2, c2, id2)),
            \/-(expr2))), 
          -\/(Literal(Bson.Null)),
          Document(c2a), id2a))
        if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, k1, id1).flatMap { case ((glbase, grbase), g) =>
            emitSt(for {
              rName <- freshName
            } yield {
              val doc = DocBuilder(g, 
                c1.content.transform { case (n, _) => -\/(DocField(n)) } +
                (rName -> \/-(grbase.toJs >>> expr2)))
              GroupBuilder(doc, -\/(Literal(Bson.Null)), 
                Document(
                  c1.content.transform { case (n, _) => -\/(DocField(n)) } ++
                  rewriteObjRefs(c2a)(prefixBase(DocField(rName)))),
                  id2a)
            })
          }
      case (
        GroupBuilderF(Term(ExprBuilderF(Term(GroupBuilderF(_, _, _, id1)), \/-(_))), -\/(Literal(Bson.Null)), Document(_), _),
        GroupBuilderF(_, _, Document(_), id2))
          if id1 == id2 =>
            delegate
      
      // NB: these cases come up when wildcards are used, and require a more general 
      // approach, which we don't know to be correct and efficient in all cases.
      case (CollectionBuilderF(_, _, _), _) => generalMerge
      case (_, CollectionBuilderF(_, _, _)) => generalMerge
      case (ExprBuilderF(_, _), ExprBuilderF(_, _)) => generalMerge

      case _ => fail(WorkflowBuilderError.InvalidOperation(
                "objectConcat",
                "unrecognized shapes:\n" + wb1.show + "\n" + wb2.show))
    }
  }

  def arrayConcat(left: WorkflowBuilder, right: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def convert(base: DocVar, cb: CollectionBuilderF, shift: Int) =
      cb.struct.simplify match {
        case SchemaChange.MakeArray(m) =>
          \/-((
            (m.keys.toSeq.map { index =>
              BsonField.Index(index + shift) -> -\/(base \ BsonField.Index(index))
            }),
            if (shift == 0) m else m.map(t => (t._1 + shift) -> t._2),
            m.keys.max + 1 + shift))
        case _ => -\/(WorkflowBuilderError.CannotObjectConcatExpr)
      }

    (left.unFix, right.unFix) match {
      case (
        cb1 @ CollectionBuilderF(_, _, _),
        cb2 @ CollectionBuilderF(_, _, _)) =>
        merge(left, right) { (left, right, list) =>
          for {
            l <- lift(convert(left, cb1, 0))
            (reshape1, array1, shift) = l
            r <- lift(convert(right, cb2, shift))
            (reshape2, array2, _) = r
            rez <- chainBuilder(
              list, DocVar.ROOT(), SchemaChange.MakeArray(array1 ++ array2))(
              $project(Reshape.Arr(ListMap((reshape1 ++ reshape2): _*))))
          } yield rez
        }
      case (ValueBuilderF(Bson.Arr(seq1)), ValueBuilderF(Bson.Arr(seq2))) =>
        emit(ValueBuilder(Bson.Arr(seq1 ++ seq2)))
      case (cb @ CollectionBuilderF(src, base, _), ValueBuilderF(Bson.Arr(seq))) =>
        lift(for {
          l <- convert(base, cb, 0)
          (reshape, array, shift) = l
        } yield CollectionBuilder(
          chain(src,
            $project(Reshape.Arr(ListMap((reshape ++ seq.zipWithIndex.map {
              case (item, index) =>
                (BsonField.Index(index + shift) -> -\/(Literal(item)))
            }): _*)))),
          DocVar.ROOT(),
          SchemaChange.MakeArray(array ++ seq.zipWithIndex.map {
            case (item, index) =>
              // TODO: make the schema for the values more specific (#437)
              (index + shift -> SchemaChange.Init)
          })))
      case (ValueBuilderF(Bson.Arr(seq)), cb @ CollectionBuilderF(src, base, _)) =>
        lift(for {
          r <- convert(base, cb, seq.length)
          (reshape, array, _) = r
        } yield CollectionBuilder(
          chain(src,
            $project(Reshape.Arr(ListMap((seq.zipWithIndex.map {
              case (item, index) =>
                (BsonField.Index(index) -> -\/(Literal(item)))
            } ++ reshape): _*)))),
          DocVar.ROOT(),
          SchemaChange.MakeArray(seq.zipWithIndex.map {
            case (item, index) =>
              // TODO: make the schema for the values more specific (#437)
              (index -> SchemaChange.Init)
          }.toListMap ++ array)))
      case _ =>
        fail(WorkflowBuilderError.InvalidOperation(
          "arrayConcat",
          "values are not both arrays"))
    }
  }

  def flattenObject(wb: WorkflowBuilder): M[WorkflowBuilder] =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(graph, base, struct) =>
        val field = base.toJs(JsCore.Ident("value").fix).toJs
        CollectionBuilder(
          chain(graph,
            $flatMap(
              Js.AnonFunDecl(List("key", "value"),
                List(
                  Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                  Js.ForIn(Js.Ident("attr"), field,
                    Js.Call(
                      Js.Select(Js.Ident("rez"), "push"),
                      List(
                        Js.AnonElem(List(
                          Js.Call(Js.Ident("ObjectId"), Nil),
                          Js.Access(field, Js.Ident("attr"))))))),
                  Js.Return(Js.Ident("rez")))))),
          DocVar.ROOT(),
          struct)
    })

  def flattenArray(wb: WorkflowBuilder): M[WorkflowBuilder] =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(graph, base, struct) =>
        CollectionBuilder(chain(graph, $unwind(base)), base, struct)
    })

  def projectField(wb: WorkflowBuilder, name: String):
      Error \/ WorkflowBuilder =
    wb.unFix match {
      case CollectionBuilderF(graph, base, SchemaChange.MakeArray(_)) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "projectField",
          "can not project a field from an array."))
      case ValueBuilderF(Bson.Doc(fields)) =>
        fields.get(name).fold[Error \/ WorkflowBuilder](
          -\/(WorkflowBuilderError.InvalidOperation(
            "projectField",
            "value does not contain a field ‘" + name + "’.")))(
          x => \/-(ValueBuilder(x)))
      case ValueBuilderF(_) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "projectField",
          "value is not a document."))
      case GroupBuilderF(wb0, key, Field(d), id) =>
        // TODO: check structure of wb0 (#436)
        \/-(GroupBuilder(wb0, key, Field(d \ BsonField.Name(name)), id))
      case ExprBuilderF(wb, -\/(DocField(field))) =>
        \/-(ExprBuilder(wb, -\/(DocField(field \ BsonField.Name(name)))))
      case _ => \/-(ExprBuilder(wb, -\/(DocField(BsonField.Name(name)))))
    }

  def projectIndex(wb: WorkflowBuilder, index: Int): M[WorkflowBuilder] =
    wb.unFix match {
      case ValueBuilderF(Bson.Arr(elems)) =>
        if (index < elems.length) // UGH!
          emit(ValueBuilder(elems(index)))
        else
          fail(WorkflowBuilderError.InvalidOperation(
            "projectIndex",
            "value does not contain index ‘" + index + "’."))
      case ValueBuilderF(_) =>
        fail(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case DocBuilderF(_, _) =>
        fail(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case _ => toCollectionBuilder(wb).map(_ match {
        // TODO: if the struct is MakeArray, we can see if the index is there
        //       and error if not. (#436)
        case CollectionBuilderF(graph, base, struct) =>
          // TODO: when https://jira.mongodb.org/browse/SERVER-4589 is fixed,
          //       replace with:
          //       ExprBuilder(wb, DocField(BsonField.Index(Index)))
          // NB: Also, would be nice to always use ExprBuilder here, and then
          //     move the special 2.6 handling to workflow generation. (#455)
          CollectionBuilder(
            chain(graph,
              $simpleMap(JsMacro(x =>
                JsCore.Access(base.toJs(x),
                  JsCore.Literal(Js.Num(index, false)).fix).fix))),
            DocVar.ROOT(),
            struct.projectIndex(index))
      })
    }

  def groupBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] =
    foldBuilders(src, keys).map { case (wb, base, fields) =>
      GroupBuilder(
        rewritePrefix(wb, base),
        keys match {
          case Nil        => -\/(Literal(Bson.Null))
          case key :: Nil => key.unFix match {
            // NB: normalize to Null, to ease merging
            case ValueBuilderF(_)            => -\/(Literal(Bson.Null))
            case ExprBuilderF(_, -\/(Literal(_))) => -\/(Literal(Bson.Null))
            case _ => -\/(fields.head)
          }
          case _          => \/-(Reshape.Arr(fields.zipWithIndex.map {
            case (field, index) =>
              BsonField.Index(index) -> -\/(field)
          }.toListMap))
        },
        Field(base),
        GroupId(wb :: keys))
    }

  def reduce(wb: WorkflowBuilder)(f: ExprOp => GroupOp): WorkflowBuilder =
    wb.unFix match {
      case GroupBuilderF(wb0, key, Field(d), id) =>
        GroupBuilder(wb0, key, Reduced(f(d)), id)
      case GroupBuilderF(wb0, key, Pushed(expr), id) =>
        GroupBuilder(wb0, key, Reduced(f(expr)), id)
      case ExprBuilderF(Term(GroupBuilderF(wb0, key, Field(d), id)), -\/(expr)) =>
        GroupBuilder(wb0, key, Reduced(f(expr.rewriteRefs(prefixBase(d)))), id)
      case _ =>
        // NB: the group must be identified with the source collection, not an
        // expression/doc built on it. This is sufficient in the known cases, 
        // but we might need to dig for an actual CollectionBuilder to be safe.
        val id = wb.unFix match {
          case ExprBuilderF(wb0, _) => GroupId(List(wb0))
          case DocBuilderF(wb0, _) => GroupId(List(wb0))
          case _ => GroupId(List(wb))
        }
        GroupBuilder(wb, -\/(Literal(Bson.Null)), Reduced(f(DocVar.ROOT())), id)
    }

  def sortBy(
    src: WorkflowBuilder, keys: List[WorkflowBuilder], sortTypes: List[SortType]):
      M[WorkflowBuilder] =
    foldBuilders(src, keys).flatMap { case (wb, base, fields) =>
      val sortFields = fields.map(_.deref).sequence.fold(
        fail[List[(BsonField, SortType)]](WorkflowBuilderError.InvalidSortBy))(
        x => emit(x.zip(sortTypes)))

      sortFields.flatMap(_ match {
        case Nil =>
          fail[WorkflowBuilder](WorkflowBuilderError.InvalidSortBy)
        case x :: xs =>
          toCollectionBuilder(wb).map(g =>
            Term[WorkflowBuilderF](g.copy(
              graph = chain(g.graph, $sort(NonEmptyList.nel(x, xs))),
              base = base)))
      })
    }

  def join(left: WorkflowBuilder, right: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey: ExprOp, rightKey: Js.Expr => Js.Expr):
      M[WorkflowBuilder] = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._

    // Note: these have to match the names used in the logical plan
    val leftField: BsonField.Name = BsonField.Name("left")
    val rightField: BsonField.Name = BsonField.Name("right")

    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): ExprOp =
      Cond(
        Eq(
          Size(DocField(side)),
          Literal(Bson.Int32(0))),
        Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
        DocField(side))

    def buildProjection(l: ExprOp, r: ExprOp):
        WorkflowOp =
      chain(_,
        $project(Reshape.Doc(ListMap(
          leftField -> -\/(l),
          rightField -> -\/(r)))))

    def buildJoin(src: Workflow, tpe: JoinType): Workflow =
      tpe match {
        case FullOuter =>
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case LeftOuter =>
          chain(src,
            $match(Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(DocField(leftField), padEmpty(rightField)))
        case RightOuter =>
          chain(src,
            $match(Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), DocField(rightField)))
        case Inner =>
          chain(
            src,
            $match(
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
      }

    def rightMap(keyExpr: Js.Expr => Js.Expr): AnonFunDecl =
      $Map.mapKeyVal(("key", "value"),
        keyExpr(Ident("value")),
        AnonObjDecl(List(
          (leftField.asText, AnonElem(Nil)),
          (rightField.asText, AnonElem(List(Ident("value")))))))

    val rightReduce =
      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              (leftField.asText, AnonElem(Nil)),
              (rightField.asText, AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), leftField.asText),
                  Call(Select(Select(Ident("result"), leftField.asText), "concat"),
                    List(Select(Ident("value"), leftField.asText)))),
                BinOp("=",
                  Select(Ident("result"), rightField.asText),
                  Call(Select(Select(Ident("result"), rightField.asText), "concat"),
                    List(Select(Ident("value"), rightField.asText)))))))),
          Return(Ident("result"))))

    comp match {
      case relations.Eq =>
        (workflow(left) |@| workflow(right)) {
          case ((l, lbase), (r, rbase)) =>
            CollectionBuilder(
              chain(
                $foldLeft(
                  chain(
                    l,
                    $group(
                      Grouped(ListMap(leftField -> Push(lbase))), -\/(leftKey.rewriteRefs(prefixBase(lbase)))),
                    Workflow.$project(
                      Reshape.Doc(ListMap(
                        leftField -> -\/(DocField(leftField)),
                        rightField -> -\/(Literal(Bson.Arr(Nil))))),
                      IncludeId)),
                  chain(r,
                    $map(rightMap(rightKey)),
                    $reduce(rightReduce))),
                buildJoin(_, tpe),
                $unwind(DocField(leftField)),
                $unwind(DocField(rightField))),
              DocVar.ROOT(),
              SchemaChange.Init)
        }
      case _ => fail(WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(left: WorkflowBuilder, right: WorkflowBuilder) =
    join(left, right,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      Literal(Bson.Null), Function.const(Js.Null))

  private def appendOp (wb: WorkflowBuilder, op: WorkflowOp):
      M[WorkflowBuilder] =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(graph, base, struct) =>
        val (newGraph, newBase) = Workflow.rewrite(op(graph).unFix, base)
        CollectionBuilder(Term(newGraph), newBase, struct)
    })

  def limit(wb: WorkflowBuilder, count: Long) = appendOp(wb, $limit(count))

  def skip(wb: WorkflowBuilder, count: Long) = appendOp(wb, $skip(count))

  def squash(wb: WorkflowBuilder): WorkflowBuilder = wb

  def distinctBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] = {
    def sortKeys(op: WorkflowBuilder): Error \/ List[(BsonField, SortType)] = {
      object HavingRoot {
        def unapply(shape: Reshape): Option[BsonField] = shape match {
          case Reshape.Doc(map) => map.collect {
            case (name, -\/(op)) if op == DocVar.ROOT() => name
          }.headOption
          case Reshape.Arr(map) => map.collect {
            case (index, -\/(op)) if op == DocVar.ROOT() => index
          }.headOption
        }
      }

      def isOrdered(op: Workflow): Boolean = op.unFix match {
        case $Sort(_, _)                            => true
        case $Group(_, _, _)                        => false
        case $GeoNear(_, _, _, _, _, _, _, _, _, _) => true
        case p: PipelineF[_]                        => isOrdered(p.src)
        case _                                      => false
      }

      // Note: this currently only handles a couple of cases, which are the ones
      // that are generated by the compiler for SQL's distinct keyword, with
      // order by, with or without "synthetic" projections. A more general
      // implementation would rewrite the pipeline to handle additional cases.
      op.unFix match {
        case CollectionBuilderF(graph, _, _) => graph.unFix match {
          case $Sort(_, keys) => \/-(keys.list)

          case $Project(Term($Sort(_, keys)), HavingRoot(root), _) =>
            \/-(keys.list.map {
              case (field, sortType) => (root \ field) -> sortType
            })

          case _ =>
            if (isOrdered(graph))
              -\/(WorkflowBuilderError.UnsupportedDistinct("cannot distinct with unrecognized ordered source: " + op))
            else \/-(Nil)
        }
        case GroupBuilderF(_, _, _, _) => \/-(Nil)
        case DocBuilderF(src, _)    => sortKeys(src)
        case ExprBuilderF(src, _)   => sortKeys(src)
        case ValueBuilderF(_)       => \/-(Nil)
      }
    }

    val distinct = foldBuilders(src, keys).map { case (merged, value, fields) =>
      lift(sortKeys(merged)).flatMap { sk =>
        val keyPrefix = "__sd_key_"
        val keyProjs = sk.zipWithIndex.map { case ((name, _), index) =>
          BsonField.Name(keyPrefix + index.toString) -> First(DocField(name))
        }
        val groupedBy = fields match {
          case Nil        => \/-(-\/(Literal(Bson.Null)))
          case key :: Nil => key match {
            // If the key is at the document root, we must explicitly
            //  project out the fields so as not to include a meaningless
            // _id in the key:
            case DocVar.ROOT(None) => keys.head.unFix match {
              case CollectionBuilderF(g2, b2, s2) =>
                s2.simplify match {
                  case SchemaChange.MakeObject(byFields) =>
                    \/-(\/-(Reshape.Arr(
                      byFields.keys.toList.zipWithIndex.map { case (name, index) =>
                        BsonField.Index(index) -> -\/(DocField(BsonField.Name(name)))
                    }.toListMap)))
                  case SchemaChange.MakeArray(byFields) =>
                    \/-(\/-(Reshape.Arr(
                      byFields.keys.toList.map { index =>
                        BsonField.Index(index) -> -\/(DocField(BsonField.Index(index)))
                      }.toListMap)))
                  case _ =>
                    -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + s2 + ")"))
              }
              case DocBuilderF(ksrc, shape) =>
                \/-(\/-(Reshape.Arr(ListMap(shape.keys.toList.zipWithIndex.map { case (name, index) => BsonField.Index(index) -> -\/(DocField(name)) }: _*))))
              case GroupBuilderF(_, _, Document(obj), _) =>
                \/-(\/-(Reshape.Arr(ListMap(obj.keys.toList.zipWithIndex.map { case (name, index) => BsonField.Index(index) -> -\/(DocField(name)) }: _*))))
              case _ => -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + keys.head + ")"))
            }
            case _                 => \/-(-\/(key))
          }
          case _          =>  \/-(\/-(Reshape.Arr(fields.zipWithIndex.map {
            case (field, index) =>
              BsonField.Index(index) -> -\/(field)
          }.toListMap)))
        }

        val group: M[CollectionBuilderF] = for {
          name <- emitSt(freshName)
          gby  <- lift(groupedBy)
          merg <- toCollectionBuilder(merged)
          CollectionBuilderF(graph, base, struct) = merg
        } yield CollectionBuilderF(
          chain(
            graph,
            $group(
              Grouped(ListMap(name -> First(base \\ value) :: keyProjs: _*)),
              gby)),
          DocField(name),
          struct)

        val keyPairs = sk.zipWithIndex.map { case ((name, sortType), index) =>
          BsonField.Name(keyPrefix + index.toString) -> sortType
        }
        keyPairs.headOption.fold(group) { head =>
          val tail = keyPairs.drop(1)
          group.map(g => g.copy(graph = chain(g.graph, $sort(NonEmptyList(head, tail: _*)))))
        }
      }
    }.join

    distinct.map(Term[WorkflowBuilderF](_))
  }

  def asExprOp(wb: WorkflowBuilder): Option[ExprOp] = wb.unFix match {
    case ValueBuilderF(value)         => Some(Literal(value))
    case ExprBuilderF(src, -\/(expr)) => Some(expr)
    case _                            => None
  }

  private def merge[A]
    (left: WorkflowBuilder, right: WorkflowBuilder)
    (f: (DocVar, DocVar, WorkflowBuilder) => M[A]):
      M[A] =
    (left.unFix, right.unFix) match {
      case (
        ExprBuilderF(src1, -\/(base1 @ DocField(_))),
        ExprBuilderF(src2, -\/(base2 @ DocField(_))))
          if src1 == src2 =>
        f(base1, base2, src1)

      case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) if src1 == src2 => 
        for {
          lName <- emitSt(freshName)
          rName <- emitSt(freshName)
          wb    <- f(DocField(lName), DocField(rName), 
            DocBuilder(src1, ListMap(
              lName -> expr1,
              rName -> expr2)))
        } yield wb

      case (ExprBuilderF(src, -\/(base @ DocField(_))), _) if src == right =>
        f(base, DocVar.ROOT(), right)
      case (_, ExprBuilderF(src, -\/(base @ DocField(_)))) if left == src =>
        f(DocVar.ROOT(), base, left)

      case (_, ExprBuilderF(src, expr)) if left == src =>
        for {
          lName <- emitSt(freshName)
          rName <- emitSt(freshName)
          wb    <- f(DocField(lName), DocField(rName), 
            DocBuilder(src, ListMap(
              lName -> -\/(DocVar.ROOT()),
              rName -> expr)))
        } yield wb
      case (ExprBuilderF(src, expr), _) if src == right =>
        for {
          lName <- emitSt(freshName)
          rName <- emitSt(freshName)
          wb    <- f(DocField(lName), DocField(rName),
            DocBuilder(src, ListMap(
              lName -> expr,
              rName -> -\/(DocVar.ROOT()))))
        } yield wb

      case (DocBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) if src1 == src2 =>
        for {
          rName <- emitSt(freshName)
          wb    <- f(DocVar.ROOT(), DocField(rName), 
            DocBuilder(src1, shape1 + (rName -> expr2)))
        } yield wb
      case (ExprBuilderF(src1, expr1), DocBuilderF(src2, shape2)) if src1 == src2 =>
        for {
          lName <- emitSt(freshName)
          wb    <- f(DocField(lName), DocVar.ROOT(),
            DocBuilder(src1, ListMap(lName -> expr1) ++ shape2))
        } yield wb

      case (DocBuilderF(src1, shape1), CollectionBuilderF(_, _, _)) if src1 == right => 
        for {
          rName <- emitSt(freshName)
          wb    <- f(DocVar.ROOT(), DocField(rName),
            DocBuilder(src1, shape1 + (rName -> -\/(DocVar.ROOT()))))
        } yield wb
      case (CollectionBuilderF(_, _, _), DocBuilderF(src2, shape2)) if left == src2 => 
        for {
          lName <- emitSt(freshName)
          wb    <- f(DocField(lName), DocVar.ROOT(),
            DocBuilder(src2, ListMap(lName -> -\/(DocVar.ROOT())) ++ shape2))
        } yield wb
        
      case (DocBuilderF(src1, shape1), DocBuilderF(src2, shape2)) =>
        merge[A](src1, src2) { (lbase, rbase, wb) =>
          Reshape.mergeMaps(
            rewriteDocPrefix(shape1, lbase),
            rewriteDocPrefix(shape2, rbase)).fold[M[A]](
            (toCollectionBuilder(left) |@| toCollectionBuilder(right))((_, _)).flatMap(_ match {
              case (
                CollectionBuilderF(graph1, base1, _),
                CollectionBuilderF(graph2, base2, _)) =>
                emitSt((Workflow.merge(graph1, graph2)).map { case ((lbase, rbase), op) =>
                  f(lbase \\ base1, rbase \\ base2,
                    CollectionBuilder(op, DocVar.ROOT(), SchemaChange.Init))
                }).join
            }))(
            x => f(DocVar.ROOT(), DocVar.ROOT(), DocBuilder(wb, x)))
        }
      case (GroupBuilderF(src1, key1, cont1, id1), GroupBuilderF(src2, key2, cont2, id2))
          if src1 == src2 && id1 == id2 =>
        def documentize(cont: GroupContents):
            MId[(ListMap[BsonField.Name, ExprOp \/ GroupOp], DocVar)] =
          cont match {
            case Field(d)         =>
              freshName.map(fn => (ListMap(fn -> -\/(d)), DocField(fn)))
            case Pushed(expr)     =>
              freshName.map(fn => (ListMap(fn -> -\/(expr)), DocField(fn)))
            case Reduced(grouped) =>
              freshName.map(fn => (ListMap(fn -> \/-(grouped)), DocField(fn)))
            case Document(obj)    => state((obj, DocVar.ROOT()))
          }

        emitSt((documentize(cont1) |@| documentize(cont2)) {
          case ((obj1, b1), (obj2, b2)) =>
            f(b1, b2, GroupBuilder(src1, key1, Document(obj1 ++ obj2), id1))
        }).join
      case _ =>
        (toCollectionBuilder(left) |@| toCollectionBuilder(right))((_, _)).flatMap(_ match {
          case (
            CollectionBuilderF(graph1, base1, struct1),
            CollectionBuilderF(graph2, base2, struct2)) =>
            emitSt(Workflow.merge(graph1, graph2).map { case ((lbase, rbase), op) =>
              f(lbase \\ base1, rbase \\ base2,
                CollectionBuilder(
                  op,
                  DocVar.ROOT(),
                  if (struct1 == struct2) struct1 else SchemaChange.Init))
            })
        }).join
    }

  def read(coll: Collection) =
    CollectionBuilder($read(coll), DocVar.ROOT(), SchemaChange.Init)
  def pure(bson: Bson) = ValueBuilder(bson)

  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[Workflow], RE: RenderTree[ExprOp]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    def renderExpr(x: Expr) = 
      x.fold(
        op => Terminal(op.toString, List("ExprBuilder", "ExprOp")),
        js => Terminal(js(JsCore.Ident("_").fix).toJs.render(0), List("ExprBuilder", "Js")))

    def render(v: WorkflowBuilder) = v.unFix match {
      case CollectionBuilderF(graph, base, struct) =>
        NonTerminal("",
          RO.render(graph) ::
            RE.render(base) ::
            Terminal(struct.toString, "CollectionBuilder" :: "SchemaChange" :: Nil) ::
            Nil,
          "CollectionBuilder" :: Nil)
      case ValueBuilderF(value) =>
        Terminal(value.toString, "ValueBuilder" :: Nil)
      case ExprBuilderF(src, expr) =>
        NonTerminal("",
          render(src) ::
            renderExpr(expr) ::
            Nil,
          List("ExprBuilder"))
      case DocBuilderF(src, shape) =>
        NonTerminal("",
          render(src) ::
            NonTerminal("", 
              shape.toList.map { case (name, expr) => renderExpr(expr).relabel(name.asText + " -> " + _) },
              List("DocBuilder", "Shape")) ::
            Nil,
          List("DocBuilder"))
      case GroupBuilderF(src, key, content, id) =>
        NonTerminal("",
          render(src) ::
            Terminal(key.toString, "GroupBuilder" :: "By" :: Nil) ::
            (content match {
              case Field(d) =>
                Terminal(d.toString, "GroupBuilder" :: "Field" :: Nil)
              case Pushed(expr) =>
                Terminal(expr.toString, "GroupBuilder" :: "Pushed" :: Nil)
              case Reduced(group) =>
                Terminal(group.toString, "GroupBuilder" :: "Reduced" :: Nil)
              case Document(obj) =>
                Terminal(obj.toString, "GroupBuilder" :: "Document" :: Nil)
            }) ::
            Terminal(id.toString, "GroupBuilder" :: "Id" :: Nil) ::
            Nil,
          "GroupBuilder" :: Nil)
    }
  }
}
