/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.recursionschemes._, Recursive.ops._
import quasar.fp._
import quasar.fs.Path
import quasar.namegen._

import scalaz._, Scalaz._, Validation.{success, failure}, Validation.FlatMap._
import shapeless.contrib.scalaz.instances.deriveEqual

sealed trait LogicalPlan[A]
object LogicalPlan {
  import quasar.std.StdLib._
  import identity._
  import set._
  import structural._

  implicit val LogicalPlanTraverse = new Traverse[LogicalPlan] {
    def traverseImpl[G[_], A, B](fa: LogicalPlan[A])(f: A => G[B])(implicit G: Applicative[G]): G[LogicalPlan[B]] = {
      fa match {
        case ReadF(coll) => G.point(ReadF(coll))
        case ConstantF(data) => G.point(ConstantF(data))
        case InvokeF(func, values) => G.map(Traverse[List].sequence(values.map(f)))(InvokeF(func, _))
        case FreeF(v) => G.point(FreeF(v))
        case LetF(ident, form0, in0) =>
          G.apply2(f(form0), f(in0))(LetF(ident, _, _))
        case TypecheckF(expr, typ, cont, fallback) =>
          G.apply3(f(expr), f(cont), f(fallback))(TypecheckF(_, typ, _, _))
      }
    }

    override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] = {
      v match {
        case ReadF(coll) => ReadF(coll)
        case ConstantF(data) => ConstantF(data)
        case InvokeF(func, values) => InvokeF(func, values.map(f))
        case FreeF(v) => FreeF(v)
        case LetF(ident, form, in) => LetF(ident, f(form), f(in))
        case TypecheckF(expr, typ, cont, fallback) =>
          TypecheckF(f(expr), typ, f(cont), f(fallback))
      }
    }

    override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit F: Monoid[B]): B = {
      fa match {
        case ReadF(_) => F.zero
        case ConstantF(_) => F.zero
        case InvokeF(func, values) => Foldable[List].foldMap(values)(f)
        case FreeF(_) => F.zero
        case LetF(_, form, in) => F.append(f(form), f(in))
        case TypecheckF(expr, _, cont, fallback) =>
          F.append(f(expr), F.append(f(cont), f(fallback)))
      }
    }

    override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B = {
      fa match {
        case ReadF(_) => z
        case ConstantF(_) => z
        case InvokeF(func, values) => Foldable[List].foldRight(values, z)(f)
        case FreeF(_) => z
        case LetF(ident, form, in) => f(form, f(in, z))
        case TypecheckF(expr, _, cont, fallback) =>
          f(expr, f(cont, f(fallback, z)))
      }
    }
  }
  implicit val RenderTreeLogicalPlan: RenderTree[LogicalPlan[_]] = new RenderTree[LogicalPlan[_]] {
    val nodeType = "LogicalPlan" :: Nil

    // Note: these are all terminals; the wrapping Fix or Cofree will use these to build nodes with children.
    def render(v: LogicalPlan[_]) = v match {
      case ReadF(name)              => Terminal("Read" :: nodeType, Some(name.pathname))
      case ConstantF(data)          => Terminal("Constant" :: nodeType, Some(data.toString))
      case InvokeF(func, _     )    => Terminal(func.mappingType.toString :: "Invoke" :: nodeType, Some(func.name))
      case FreeF(name)              => Terminal("Free" :: nodeType, Some(name.toString))
      case LetF(ident, _, _)        => Terminal("Let" :: nodeType, Some(ident.toString))
      case TypecheckF(_, typ, _, fb) => Terminal("Typecheck" :: nodeType, Some(typ.toString + " ∨ " + fb.toString))
    }
  }
  implicit val EqualFLogicalPlan = new EqualF[LogicalPlan] {
    def equal[A: Equal](v1: LogicalPlan[A], v2: LogicalPlan[A]): Boolean = (v1, v2) match {
      case (ReadF(n1), ReadF(n2)) => n1 ≟ n2
      case (ConstantF(d1), ConstantF(d2)) => d1 == d2
      case (InvokeF(f1, v1), InvokeF(f2, v2)) => f1 == f2 && v1 ≟ v2
      case (FreeF(n1), FreeF(n2)) => n1 ≟ n2
      case (LetF(ident1, form1, in1), LetF(ident2, form2, in2)) =>
        ident1 ≟ ident2 && form1 ≟ form2 && in1 ≟ in2
      case (TypecheckF(expr1, typ1, cont1, fb1), TypecheckF(expr2, typ2, cont2, fb2)) =>
        expr1 ≟ expr2 && typ1 == typ2 && cont1 ≟ cont2 && fb1 ≟ fb2
      case _ => false
    }
  }

  final case class ReadF[A](path: Path) extends LogicalPlan[A] {
    override def toString = s"""Read(Path("${path.simplePathname}"))"""
  }
  object Read {
    def apply(path: Path): Fix[LogicalPlan] =
      Fix[LogicalPlan](new ReadF(path))
  }

  final case class ConstantF[A](data: Data) extends LogicalPlan[A]
  object Constant {
    def apply(data: Data): Fix[LogicalPlan] =
      Fix[LogicalPlan](ConstantF(data))
  }

  final case class InvokeF[A](func: Func, values: List[A]) extends LogicalPlan[A] {
    override def toString = {
      val funcName = if (func.name(0).isLetter) func.name.split('_').map(_.toLowerCase.capitalize).mkString
                      else "\"" + func.name + "\""
      funcName + "(" + values.mkString(", ") + ")"
    }
  }
  object Invoke {
    def apply(func: Func, values: List[Fix[LogicalPlan]]): Fix[LogicalPlan] =
      Fix[LogicalPlan](InvokeF(func, values))
  }

  final case class FreeF[A](name: Symbol) extends LogicalPlan[A]
  object Free {
    def apply(name: Symbol): Fix[LogicalPlan] =
      Fix[LogicalPlan](FreeF(name))
  }

  final case class LetF[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]
  object Let {
    def apply(let: Symbol, form: Fix[LogicalPlan], in: Fix[LogicalPlan]): Fix[LogicalPlan] =
      Fix[LogicalPlan](LetF(let, form, in))
  }

  // NB: This should only be inserted by the type checker. In future, this
  //     should only exist in SlamScript – the checker will annotate nodes
  //     where runtime checks are necessary, then they will be added during
  //     compilation to SlamScript.
  final case class TypecheckF[A](expr: A, typ: Type, cont: A, fallback: A)
      extends LogicalPlan[A]
  object Typecheck {
    def apply(expr: Fix[LogicalPlan], typ: Type, cont: Fix[LogicalPlan], fallback: Fix[LogicalPlan]):
        Fix[LogicalPlan] =
      Fix[LogicalPlan](TypecheckF(expr, typ, cont, fallback))
  }

  implicit val LogicalPlanUnzip = new Unzip[LogicalPlan] {
    def unzip[A, B](f: LogicalPlan[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val LogicalPlanBinder = new Binder[LogicalPlan] {
      type G[A] = Map[Symbol, A]

      def initial[A] = Map[Symbol, A]()

      def bindings[A](t: LogicalPlan[Fix[LogicalPlan]], b: G[A])(f: LogicalPlan[Fix[LogicalPlan]] => A): G[A] =
        t match {
          case LetF(ident, form, _) => b + (ident -> f(form.unFix))
          case _                    => b
        }

      def subst[A](t: LogicalPlan[Fix[LogicalPlan]], b: G[A]): Option[A] =
        t match {
          case FreeF(symbol) => b.get(symbol)
          case _             => None
        }
    }

  def freshName(prefix: String): State[NameGen, Symbol] =
    quasar.namegen.freshName(prefix).map(Symbol(_))

  val shapeƒ: LogicalPlan[(Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]])] => Option[List[Fix[LogicalPlan]]] = {
    case LetF(_, _, body) => body._2
    case ConstantF(Data.Obj(map)) =>
      Some(map.keys.map(n => Constant(Data.Str(n))).toList)
    case InvokeF(DeleteField, List(src, field)) =>
      src._2.map(_.filterNot(_ ≟ field._1))
    case InvokeF(MakeObject, List(field, src)) => Some(List(field._1))
    case InvokeF(ObjectConcat, srcs) => srcs.map(_._2).sequence.map(_.flatten)
    // NB: the remaining InvokeF cases simply pass through or combine shapes
    //     from their inputs. It would be great if this information could be
    //     handled generically by the type system.
    case InvokeF(OrderBy, List(src, _, _)) => src._2
    case InvokeF(Take, List(src, _)) => src._2
    case InvokeF(Drop, List(src, _)) => src._2
    case InvokeF(Filter, List(src, _)) => src._2
    case InvokeF(InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin, _)
        => Some(List(Constant(Data.Str("left")), Constant(Data.Str("right"))))
    case InvokeF(GroupBy, List(src, _)) => src._2
    case InvokeF(Distinct, List(src, _)) => src._2
    case InvokeF(DistinctBy, List(src, _)) => src._2
    case InvokeF(Squash, List(src)) => src._2
    case TypecheckF(_, _, cont, _) => cont._2
    case _ => None
  }

  type TypeFree[F[_]] = Cofree[F, Type]
  type NamedConstraint = (Symbol, Type, Fix[LogicalPlan])
  type ConstrainedPlan = (Type, List[NamedConstraint], Fix[LogicalPlan])
  type SemValidation[A] = ValidationNel[SemanticError, A]
  type SemDisj[A] = NonEmptyList[SemanticError] \/ A

  def inferTypes(typ: Type, term: Fix[LogicalPlan]):
      SemValidation[TypeFree[LogicalPlan]] =
    (term.unFix match {
      case ReadF(c)          => success(ReadF[TypeFree[LogicalPlan]](c))
      case ConstantF(d)      => success(ConstantF[TypeFree[LogicalPlan]](d))
      case InvokeF(f, args)  => for {
        types <- f.untype(typ)
        args0 <- types.zip(args).map((inferTypes(_, _)).tupled).sequenceU
      } yield InvokeF[TypeFree[LogicalPlan]](f, args0)
      case FreeF(n)          => success(FreeF[TypeFree[LogicalPlan]](n))
      case LetF(n, form, in) =>
        inferTypes(typ, in).flatMap { in0 =>
          val fTyp = in0.collect {
            case Cofree(typ0, FreeF(n0)) if n0 == n => typ0
          }.concatenate(Type.TypeGlbMonoid)
          inferTypes(fTyp, form).map(LetF[TypeFree[LogicalPlan]](n, _, in0))
        }
      case TypecheckF(expr, t, cont, fallback) =>
        (inferTypes(t, expr) |@| inferTypes(typ, cont) |@| inferTypes(typ, fallback))(
          TypecheckF[TypeFree[LogicalPlan]](_, t, _, _))
    }).map(Cofree(typ, _))

  private def lift[A](v: SemDisj[A]): NameT[SemDisj, A] =
    quasar.namegen.lift[SemDisj](v)

  /** This function compares the inferred (required) type with the possible type
    * from the collection.
    * • if it’s a const type, replace the node with a constant
    * • if the possible is a subtype of the inferred, we’re good
    * • if the inferred is a subtype of the possible, we need a runtime check
    * • otherwise, we fail
    */
  private def maybeWrap(inf: Type, poss: Type, term: Fix[LogicalPlan]):
      NameT[SemDisj, ConstrainedPlan] = {
    if (inf.contains(poss))
      emit((poss, Nil, poss match {
        case Type.Const(d) => Constant(d)
        case _ => term
      }))
    else if (poss.contains(inf)) {
      emitName(freshName("check").map(name =>
        (inf, List((name, inf, term)), Free(name))))
    }
    else lift(-\/(NonEmptyList(SemanticError.GenericError(s"couldn’t unify inferred (${inf}) and possible (${poss}) types in $term"))))
  }

  private def appConst(constraints: ConstrainedPlan, fallback: Fix[LogicalPlan]) =
    constraints._2.foldLeft(constraints._3)((acc, con) =>
      Let(con._1, con._3, Typecheck(Free(con._1), con._2, acc, fallback)))

  /** This inserts a constraint on a node that might not strictly require a type
    * check. It protects operations (EG, array flattening) that need a certain
    * shape.
    */
  private def ensureConstraint(constraints: ConstrainedPlan, fallback: Fix[LogicalPlan]): State[NameGen, Fix[LogicalPlan]] = {
    val (typ, consts, term) = constraints
      (consts match {
        case Nil =>
          freshName("check").map(name => (typ, List((name, typ, term)), Free(name)))
        case _   => constraints.point[State[NameGen, ?]]
      }).map(appConst(_, fallback))
  }

  // TODO: This can perhaps be decomposed into separate folds for annotating
  //       with “found” types, folding constants, and adding runtime checks.
  val checkTypesƒ:
      (Type, LogicalPlan[ConstrainedPlan]) => NameT[SemDisj, ConstrainedPlan] =
    (inf, term) => {
      def applyConstraints(
        poss: Type, constraints: ConstrainedPlan)
        (f: Fix[LogicalPlan] => Fix[LogicalPlan]) =
        maybeWrap(inf, poss, f(appConst(constraints, Constant(Data.NA))))

      term match {
        case ReadF(c)         => maybeWrap(inf, Type.Top, Read(c))
        case ConstantF(d)     => maybeWrap(inf, Type.Const(d), Constant(d))
        case InvokeF(MakeObject, List(name, value)) =>
          lift(MakeObject.apply(List(name._1, value._1)).disjunction).flatMap(
            applyConstraints(_, value)(MakeObject(name._3, _)))
        case InvokeF(MakeArray, List(value)) =>
          lift(MakeArray.apply(List(value._1)).disjunction).flatMap(
            applyConstraints(_, value)(MakeArray(_)))
        // TODO: Move this case to the Mongo planner once type information is
        //       available there.
        case InvokeF(ConcatOp, args) =>
          val (types, constraints, terms) = args.unzip3
          lift(ConcatOp.apply(types).disjunction).flatMap[NameGen, ConstrainedPlan](poss => poss match {
            case Type.Str         => maybeWrap(inf, poss, Invoke(string.Concat, terms))
            case t if t.arrayLike => maybeWrap(inf, poss, Invoke(ArrayConcat, terms))
            case _                => lift(-\/(NonEmptyList(SemanticError.GenericError("can't concat mixed/unknown types"))))
          }).map {
            case (a, b, c) => (a, constraints.foldLeft(b)(_ ++ _), c)
          }
        case InvokeF(relations.Or, args) =>
          lift(relations.Or.apply(args.map(_._1)).disjunction).flatMap(maybeWrap(inf, _, Invoke(relations.Or, args.map(appConst(_, Constant(Data.NA))))))
        case InvokeF(structural.FlattenArray, args) =>
          for {
            types <- lift(structural.FlattenArray.apply(args.map(_._1)).disjunction)
            consts <- emitName[SemDisj, List[Fix[LogicalPlan]]](args.map(ensureConstraint(_, Constant(Data.Arr(List(Data.NA))))).sequenceU)
            plan  <- maybeWrap(inf, types, Invoke(structural.FlattenArray, consts))
          } yield plan
        case InvokeF(structural.FlattenObject, args) => for {
          types <- lift(structural.FlattenObject.apply(args.map(_._1)).disjunction)
          consts <- emitName[SemDisj, List[Fix[LogicalPlan]]](args.map(ensureConstraint(_, Constant(Data.Obj(Map("" -> Data.NA))))).sequenceU)
          plan  <- maybeWrap(inf, types, Invoke(structural.FlattenObject, consts))
        } yield plan
        case InvokeF(f @ Mapping(_, _, _, _, _, _, _), args) =>
          val (types, constraints, terms) = args.unzip3
          lift(f.apply(types).disjunction).flatMap(maybeWrap(inf, _, Invoke(f, terms))).map {
            case (a, b, c) => (a, constraints.foldLeft(b)(_ ++ _), c)
          }
        case InvokeF(f, args) =>
          lift(f.apply(args.map(_._1)).disjunction).flatMap(maybeWrap(inf, _, Invoke(f, args.map(appConst(_, Constant(Data.NA))))))
        case TypecheckF(expr, typ, cont, fallback) =>
          maybeWrap(inf, Type.glb(cont._1, typ), Typecheck(expr._3, typ, cont._3, fallback._3))
        case LetF(name, value, in) =>
          maybeWrap(inf, in._1, Let(name, appConst(value, Constant(Data.NA)), appConst(in, Constant(Data.NA))))
        // TODO: Get the possible type from the LetF
        case FreeF(v) => emit((inf, Nil, Free(v)))
      }
    }

  type SemNames[A] = NameT[SemDisj, A]

  def ensureCorrectTypes(term: Fix[LogicalPlan]):
      ValidationNel[SemanticError, Fix[LogicalPlan]] =
    inferTypes(Type.Top, term).flatMap(
      cofCataM[LogicalPlan, SemNames, Type, ConstrainedPlan](_)(checkTypesƒ(_, _)).map(appConst(_, Constant(Data.NA))).evalZero.validation)

  // TODO: Generalize this to Binder
  def lpParaZygoHistoM[M[_]: Monad, A, B](
    t: Fix[LogicalPlan])(
    f: LogicalPlan[(Fix[LogicalPlan], B)] => B,
    g: LogicalPlan[Cofree[LogicalPlan, (B, A)]] => M[A]):
      M[A] = {
    def loop(t: Fix[LogicalPlan], bind: Map[Symbol, Cofree[LogicalPlan, (B, A)]]):
        M[Cofree[LogicalPlan, (B, A)]] = {
      lazy val default: M[Cofree[LogicalPlan, (B, A)]] = for {
        lp <- (t.unFix.map(x => for {
          co <- loop(x, bind)
        } yield ((x, co.head._1), co))).sequence
        (xb, co) = lp.unfzip
        b = f(xb)
        a <- g(co)
      } yield Cofree((b, a), co)

      t.unFix match {
        case FreeF(name)            => bind.get(name).fold(default)(_.point[M])
        case LetF(name, form, body) => for {
          form1 <- loop(form, bind)
          rez   <- loop(body, bind + (name -> form1))
        } yield rez
        case _                      => default
      }
    }

    for {
      rez <- loop(t, Map())
    } yield rez.head._2
  }

  def lpParaZygoHistoS[S, A, B] = lpParaZygoHistoM[State[S, ?], A, B] _
  def lpParaZygoHisto[A, B] = lpParaZygoHistoM[Id, A, B] _
}
