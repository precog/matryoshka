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
import quasar.recursionschemes._, cofree._, Recursive.ops._, FunctorT.ops._
import quasar.fp._
import quasar.fs.Path
import quasar.namegen._

import scalaz._, Scalaz._, Validation.{success, failure}, Validation.FlatMap._
import shapeless.contrib.scalaz.instances.deriveEqual

sealed trait LogicalPlan[A]
object LogicalPlan {
  import quasar.std.StdLib._
  import structural._

  implicit val LogicalPlanTraverse: Traverse[LogicalPlan] =
    new Traverse[LogicalPlan] {
      def traverseImpl[G[_], A, B](
        fa: LogicalPlan[A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[LogicalPlan[B]] =
        fa match {
          case ReadF(coll)           => G.point(ReadF(coll))
          case ConstantF(data)       => G.point(ConstantF(data))
          case InvokeF(func, values) => values.traverse(f).map(InvokeF(func, _))
          case FreeF(v)              => G.point(FreeF(v))
          case LetF(ident, form, in) => (f(form) ⊛ f(in))(LetF(ident, _, _))
          case TypecheckF(expr, typ, cont, fallback) =>
            (f(expr) ⊛ f(cont) ⊛ f(fallback))(TypecheckF(_, typ, _, _))
        }

      override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] =
        v match {
          case ReadF(coll)           => ReadF(coll)
          case ConstantF(data)       => ConstantF(data)
          case InvokeF(func, values) => InvokeF(func, values.map(f))
          case FreeF(v)              => FreeF(v)
          case LetF(ident, form, in) => LetF(ident, f(form), f(in))
          case TypecheckF(expr, typ, cont, fallback) =>
            TypecheckF(f(expr), typ, f(cont), f(fallback))
        }

      override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit B: Monoid[B]): B =
        fa match {
          case ReadF(_)              => B.zero
          case ConstantF(_)          => B.zero
          case InvokeF(func, values) => values.foldMap(f)
          case FreeF(_)              => B.zero
          case LetF(_, form, in)     => f(form) ⊹ f(in)
          case TypecheckF(expr, _, cont, fallback) =>
            f(expr) ⊹ f(cont) ⊹ f(fallback)
        }

      override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B =
        fa match {
          case ReadF(_)              => z
          case ConstantF(_)          => z
          case InvokeF(func, values) => Foldable[List].foldRight(values, z)(f)
          case FreeF(_)              => z
          case LetF(ident, form, in) => f(form, f(in, z))
          case TypecheckF(expr, _, cont, fallback) =>
            f(expr, f(cont, f(fallback, z)))
        }
    }
  implicit val RenderTreeLogicalPlan:
      RenderTree ~> λ[α => RenderTree[LogicalPlan[α]]] =
    new (RenderTree ~> λ[α => RenderTree[LogicalPlan[α]]]) {
      def apply[α](ra: RenderTree[α]): RenderTree[LogicalPlan[α]] =
        new RenderTree[LogicalPlan[α]] {
          val nodeType = "LogicalPlan" :: Nil

          def render(v: LogicalPlan[α]) = v match {
            // NB: a couple of special cases for readability
            case ConstantF(Data.Str(str)) => Terminal("Str" :: "Constant" :: nodeType, Some(str.shows))
            case InvokeF(structural.ObjectProject, expr :: name :: Nil) =>
              (ra.render(expr), ra.render(name)) match {
                case (RenderedTree(_, Some(x), Nil), RenderedTree(_, Some(n), Nil)) =>
                  Terminal("ObjectProject" :: nodeType, Some(x + "[" + n + "]"))
                case (x, n) => NonTerminal("Invoke" :: nodeType, Some(ObjectProject.name), x :: n :: Nil)
              }

            case ReadF(name)              => Terminal("Read" :: nodeType, Some(name.pathname))
            case ConstantF(data)          => Terminal("Constant" :: nodeType, Some(data.toString))
            case InvokeF(func, args)      => NonTerminal("Invoke" :: nodeType, Some(func.name), args.map(ra.render))
            case FreeF(name)              => Terminal("Free" :: nodeType, Some(name.toString))
            case LetF(ident, form, body)  => NonTerminal("Let" :: nodeType, Some(ident.toString), List(ra.render(form), ra.render(body)))
            case TypecheckF(expr, typ, cont, fallback) =>
              NonTerminal("Typecheck" :: nodeType, Some(typ.shows),
                List(ra.render(expr), ra.render(cont), ra.render(fallback)))
          }
        }
    }
  implicit val EqualFLogicalPlan: EqualF[LogicalPlan] =
    new EqualF[LogicalPlan] {
      def equal[A: Equal](v1: LogicalPlan[A], v2: LogicalPlan[A]): Boolean =
        (v1, v2) match {
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
  //     should only exist in BlackShield – the checker will annotate nodes
  //     where runtime checks are necessary, then they will be added during
  //     compilation to BlackShield.
  final case class TypecheckF[A](expr: A, typ: Type, cont: A, fallback: A)
      extends LogicalPlan[A]
  object Typecheck {
    def apply(expr: Fix[LogicalPlan], typ: Type, cont: Fix[LogicalPlan], fallback: Fix[LogicalPlan]):
        Fix[LogicalPlan] =
      Fix[LogicalPlan](TypecheckF(expr, typ, cont, fallback))
  }

  implicit val LogicalPlanUnzip: Unzip[LogicalPlan] = new Unzip[LogicalPlan] {
    def unzip[A, B](f: LogicalPlan[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val LogicalPlanBinder: Binder[LogicalPlan] = new Binder[LogicalPlan] {
      type G[A] = Map[Symbol, A]
      val G = implicitly[Traverse[G]]

      def initial[A] = Map[Symbol, A]()

      def bindings[T[_[_]]: Recursive, A](t: LogicalPlan[T[LogicalPlan]], b: G[A])(f: LogicalPlan[T[LogicalPlan]] => A): G[A] =
        t match {
          case LetF(ident, form, _) => b + (ident -> f(form.project))
          case _                    => b
        }

      def subst[T[_[_]]: Recursive, A](t: LogicalPlan[T[LogicalPlan]], b: G[A]): Option[A] =
        t match {
          case FreeF(symbol) => b.get(symbol)
          case _             => None
        }
    }

  def freshName(prefix: String): State[NameGen, Symbol] =
    quasar.namegen.freshName(prefix).map(Symbol(_))

  // NB: this can't currently be generalized to Binder, because the key type isn't exposed there.
  def renameƒ[M[_]: Monad](f: Symbol => M[Symbol]):
      ((Map[Symbol, Symbol], Fix[LogicalPlan])) =>
        M[LogicalPlan[(Map[Symbol, Symbol], Fix[LogicalPlan])]] =
  { case (bound, t) =>
    t.unFix match {
      case LetF(sym, expr, body) =>
        f(sym).map(sym1 =>
          LetF(sym1,
            (bound, expr),
            (bound + (sym -> sym1), body)))
      case FreeF(sym) =>
        val v: LogicalPlan[(Map[Symbol, Symbol], Fix[LogicalPlan])] =
          FreeF(bound.get(sym).getOrElse(sym))
        v.point[M]
      case t =>
        t.strengthL(bound).point[M]
    }
  }

  def rename[M[_]: Monad](f: Symbol => M[Symbol])(t: Fix[LogicalPlan]): M[Fix[LogicalPlan]] =
    (Map[Symbol, Symbol](), t).anaM(renameƒ(f))

  def normalizeTempNames(t: Fix[LogicalPlan]) =
    rename[State[NameGen, ?]](κ(freshName("tmp")))(t).evalZero

  val normalizeLetsƒ: LogicalPlan[Fix[LogicalPlan]] => Option[LogicalPlan[Fix[LogicalPlan]]] = {
      case LetF(b, Fix(LetF(a, x1, x2)), x3) => LetF(a, x1, Let(b, x2, x3)).some
      case t => None
  }

  def normalizeLets(t: Fix[LogicalPlan]) = t.transAna(repeatedly(normalizeLetsƒ))

  type Typed[F[_]] = Cofree[F, Type]
  final case class NamedConstraint(name: Symbol, inferred: Type, term: Fix[LogicalPlan])
  final case class ConstrainedPlan(inferred: Type, constraints: List[NamedConstraint], plan: Fix[LogicalPlan])
  type SemValidation[A] = ValidationNel[SemanticError, A]
  type SemDisj[A] = NonEmptyList[SemanticError] \/ A

  def inferTypes(typ: Type, term: Fix[LogicalPlan]):
      SemValidation[Typed[LogicalPlan]] =
    (term.unFix match {
      case ReadF(c)          => success(ReadF[Typed[LogicalPlan]](c))
      case ConstantF(d)      => success(ConstantF[Typed[LogicalPlan]](d))
      case InvokeF(f, args)  => for {
        types <- f.untype(typ)
        args0 <- types.zip(args).traverseU((inferTypes(_, _)).tupled)
      } yield InvokeF[Typed[LogicalPlan]](f, args0)
      case FreeF(n)          => success(FreeF[Typed[LogicalPlan]](n))
      case LetF(n, form, in) =>
        inferTypes(typ, in).flatMap { in0 =>
          val fTyp = in0.collect {
            case Cofree(typ0, FreeF(n0)) if n0 == n => typ0
          }.concatenate(Type.TypeGlbMonoid)
          inferTypes(fTyp, form).map(LetF[Typed[LogicalPlan]](n, _, in0))
        }
      case TypecheckF(expr, t, cont, fallback) =>
        (inferTypes(t, expr) ⊛ inferTypes(typ, cont) ⊛ inferTypes(typ, fallback))(
          TypecheckF[Typed[LogicalPlan]](_, t, _, _))
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
  private def unifyOrCheck(inf: Type, poss: Type, term: Fix[LogicalPlan]):
      NameT[SemDisj, ConstrainedPlan] = {
    if (inf.contains(poss))
      emit(ConstrainedPlan(poss, Nil, poss match {
        case Type.Const(d) => Constant(d)
        case _ => term
      }))
    else if (poss.contains(inf)) {
      emitName(freshName("check").map(name =>
        ConstrainedPlan(inf, List(NamedConstraint(name, inf, term)), Free(name))))
    }
    else lift(SemanticError.GenericError(s"couldn’t unify inferred (${inf}) and possible (${poss}) types in $term").wrapNel.left)
  }

  private def appConst(constraints: ConstrainedPlan, fallback: Fix[LogicalPlan]) =
    constraints.constraints.foldLeft(constraints.plan)((acc, con) =>
      Let(con.name, con.term,
        Typecheck(Free(con.name), con.inferred, acc, fallback)))

  /** This inserts a constraint on a node that might not strictly require a type
    * check. It protects operations (EG, array flattening) that need a certain
    * shape.
    */
  private def ensureConstraint(constraints: ConstrainedPlan, fallback: Fix[LogicalPlan]): State[NameGen, Fix[LogicalPlan]] = {
    val ConstrainedPlan(typ, consts, term) = constraints
      (consts match {
        case Nil =>
          freshName("check").map(name =>
            ConstrainedPlan(typ, List(NamedConstraint(name, typ, term)), Free(name)))
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
        unifyOrCheck(inf, poss, f(appConst(constraints, Constant(Data.NA))))

      term match {
        case ReadF(c)         => unifyOrCheck(inf, Type.Top, Read(c))
        case ConstantF(d)     => unifyOrCheck(inf, Type.Const(d), Constant(d))
        case InvokeF(MakeObject, List(name, value)) =>
          lift(MakeObject.apply(List(name.inferred, value.inferred)).disjunction).flatMap(
            applyConstraints(_, value)(x => Fix(MakeObject(name.plan, x))))
        case InvokeF(MakeArray, List(value)) =>
          lift(MakeArray.apply(List(value.inferred)).disjunction).flatMap(
            applyConstraints(_, value)(x => Fix(MakeArray(x))))
        // TODO: Move this case to the Mongo planner once type information is
        //       available there.
        case InvokeF(ConcatOp, args) =>
          val (types, constraints, terms) = args.foldMap(a =>
            (List(a.inferred), a.constraints, List(a.plan)))
          lift(ConcatOp.apply(types).disjunction).flatMap[NameGen, ConstrainedPlan](poss => poss match {
            case Type.Str         => unifyOrCheck(inf, poss, Invoke(string.Concat, terms))
            case t if t.arrayLike => unifyOrCheck(inf, poss, Invoke(ArrayConcat, terms))
            case _                => lift(-\/(NonEmptyList(SemanticError.GenericError("can't concat mixed/unknown types"))))
          }).map(cp =>
            cp.copy(constraints = cp.constraints ++ constraints))
        case InvokeF(relations.Or, args) =>
          lift(relations.Or.apply(args.map(_.inferred)).disjunction).flatMap(unifyOrCheck(inf, _, Invoke(relations.Or, args.map(appConst(_, Constant(Data.NA))))))
        case InvokeF(structural.FlattenArray, args) =>
          for {
            types <- lift(structural.FlattenArray.apply(args.map(_.inferred)).disjunction)
            consts <- emitName[SemDisj, List[Fix[LogicalPlan]]](args.map(ensureConstraint(_, Constant(Data.Arr(List(Data.NA))))).sequenceU)
            plan  <- unifyOrCheck(inf, types, Invoke(structural.FlattenArray, consts))
          } yield plan
        case InvokeF(structural.FlattenMap, args) => for {
          types <- lift(structural.FlattenMap.apply(args.map(_.inferred)).disjunction)
          consts <- emitName[SemDisj, List[Fix[LogicalPlan]]](args.map(ensureConstraint(_, Constant(Data.Obj(Map("" -> Data.NA))))).sequenceU)
          plan  <- unifyOrCheck(inf, types, Invoke(structural.FlattenMap, consts))
        } yield plan
        case InvokeF(f @ Mapping(_, _, _, _, _, _, _), args) =>
          val (types, constraints, terms) = args.foldMap(a =>
            (List(a.inferred), a.constraints, List(a.plan)))
          lift(f.apply(types).disjunction).flatMap(unifyOrCheck(inf, _, Invoke(f, terms))).map(cp =>
            cp.copy(constraints = cp.constraints ++ constraints))
        case InvokeF(f, args) =>
          lift(f.apply(args.map(_.inferred)).disjunction).flatMap(unifyOrCheck(inf, _, Invoke(f, args.map(appConst(_, Constant(Data.NA))))))
        case TypecheckF(expr, typ, cont, fallback) =>
          unifyOrCheck(inf, Type.glb(cont.inferred, typ), Typecheck(expr.plan, typ, cont.plan, fallback.plan))
        case LetF(name, value, in) =>
          unifyOrCheck(inf, in.inferred, Let(name, appConst(value, Constant(Data.NA)), appConst(in, Constant(Data.NA))))
        // TODO: Get the possible type from the LetF
        case FreeF(v) => emit(ConstrainedPlan(inf, Nil, Free(v)))
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
