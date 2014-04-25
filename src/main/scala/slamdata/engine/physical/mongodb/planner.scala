package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.std.StdLib._

import scalaz.{Free => FreeM, Node => _, _}
import scalaz.task.Task

import scalaz.syntax.either._
import scalaz.syntax.compose._
import scalaz.syntax.applicativePlus._

import scalaz.std.option._
import scalaz.std.anyVal._

trait MongoDbPlanner2 {
  import LogicalPlan2._

  import slamdata.engine.analysis.fixplate._

  import set._
  import relations._
  import structural._
  import math._

  /**
   * This phase works bottom-up to assemble sequences of object dereferences into
   * the format required by MongoDB -- e.g. "foo.bar.baz".
   *
   * This is necessary because MongoDB does not treat object dereference as a 
   * first-class binary operator, and the resulting irregular structure cannot
   * be easily generated without computing this intermediate.
   *
   * This annotation can also be used to help detect two spans of dereferences
   * separated by non-dereference operations. Such "broken" dereferences cannot
   * be translated into a single pipeline operation and require 3 pipeline 
   * operations: [dereference, middle op, dereference].
   */
  def FieldPhase[A]: PhaseE[LogicalPlan2, PlannerError, A, Option[BsonField]] = {
    type FieldPhaseAttr = Option[BsonField]
    
    liftPhaseE(Phase { (attr: LPAttr[A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan2[(LPTerm, FieldPhaseAttr)]) =>
        node.fold[FieldPhaseAttr](
          read      = Function.const(None), 
          constant  = Function.const(None),
          join      = (left, right, tpe, rel, lproj, rproj) => None,
          invoke    = (func, args) => 
                      if (func == ObjectProject) {
                        val (objTerm, objAttrOpt) :: (Term(LogicalPlan2.Constant(Data.Str(fieldName))), None) :: Nil = args

                        Some(objAttrOpt match {
                          case Some(objAttr) =>
                            objAttr :+ BsonField.Name(fieldName)

                          case None =>
                            BsonField.Name(fieldName)
                        })
                      } else {
                        None
                      }
        )
      }
    })
  }

  /**
   * This phase builds up expression operations from field attributes.
   *
   * As it works its way up the tree, at some point, it will reach a place where
   * the value cannot be computed as an expression operation. The phase will produce
   * None at these points. Further up the tree from such a position, it may again
   * be possible to build expression operations, so this process will naturally
   * result in spans of expressions alternating with spans of nothing (i.e. None).
   *
   * The "holes" represent positions where a pipeline operation or even a workflow
   * task is required to compute the given expression.
   */
  def ExprPhase: PhaseE[LogicalPlan2, PlannerError, Option[BsonField], Option[ExprOp]] = {
    type ExprPhaseAttr = PlannerError \/ Option[ExprOp]

    toPhaseE(Phase { (attr: LPAttr[Option[BsonField]]) =>
      scanCata(attr) { (fieldAttr: Option[BsonField], node: LogicalPlan2[ExprPhaseAttr]) =>
        def emit(expr: ExprOp): ExprPhaseAttr = \/- (Some(expr))

        def promoteBsonField = \/- (fieldAttr.map(ExprOp.DocField.apply _))

        def nothing = \/- (None)

        def invoke(func: Func, args: List[ExprPhaseAttr]): ExprPhaseAttr = {
          def invoke1(f: ExprOp => ExprOp) = {
            val x :: Nil = args

            x.map(_.map(f))
          }
          def invoke2(f: (ExprOp, ExprOp) => ExprOp) = {
            val x :: y :: Nil = args

            (x |@| y)(f)
          }

          func match {
            case `Add`      => invoke2(ExprOp.Add.apply _)
            case `Multiply` => invoke2(ExprOp.Multiply.apply _)
            case `Subtract` => invoke2(ExprOp.Subtract.apply _)
            case `Divide`   => invoke2(ExprOp.Divide.apply _)

            case `Eq`       => invoke2(ExprOp.Eq.apply _)
            case `Neq`      => invoke2(ExprOp.Neq.apply _)
            case `Lt`       => invoke2(ExprOp.Lt.apply _)
            case `Lte`      => invoke2(ExprOp.Lte.apply _)
            case `Gt`       => invoke2(ExprOp.Gt.apply _)
            case `Gte`      => invoke2(ExprOp.Gte.apply _)

            case `ObjectProject`  => promoteBsonField
            case `ArrayProject`   => promoteBsonField

            case _ => nothing
          }
        }

        node.fold[ExprPhaseAttr](
          read      = _ => promoteBsonField, // FIXME: Need to descend into appropriate join
          constant  = data => Bson.fromData(data).bimap[PlannerError, Option[ExprOp]](
                        _ => PlannerError.NonRepresentableData(data), 
                        d => Some(ExprOp.Literal(d))
                      ),
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _)
        )
      }
    })
  }

  /**
   * The selector phase tries to turn expressions into MongoDB selectors -- i.e. 
   * Mongo query expressions. Selectors are only used for the filtering pipeline op,
   * so it's quite possible we build more stuff than is needed (but it doesn't matter, 
   * unneeded annotations will be ignored by the pipeline phase).
   *
   * Like the expression op phase, this one requires bson field annotations.
   *
   * Most expressions cannot be turned into selector expressions without using the
   * "$where" operator, which allows embedding JavaScript code. Unfortunately, using
   * this operator turns filtering into a full table scan. We should do a pass over
   * the tree to identify partial boolean expressions which can be turned into selectors,
   * factoring out the leftovers for conversion using Where.
   *
   */
  def SelectorPhase: PhaseE[LogicalPlan2, PlannerError, Option[BsonField], Option[Selector]] = {
    type Input = Option[BsonField]
    type Output = Option[Selector]

    liftPhaseE(Phase { (attr: LPAttr[Input]) =>
      scanPara2(attr) { (fieldAttr: Input, node: LogicalPlan2[(Term[LogicalPlan2], Input, Output)]) =>
        def emit(sel: Selector): Output = Some(sel)

        def promoteBsonField = fieldAttr.map(???)

        def invoke(func: Func, args: List[(Term[LogicalPlan2], Input, Output)]): Output = {
          /**
           * Attempts to extract a BsonField annotation and a selector from
           * an argument list of length two.
           */
          def extractFieldAndSelector: Option[(BsonField, Selector)] = {
            val (_, f1, s1) :: (_, f2, s2) :: Nil = args

            f1.map((_, s2)).orElse(f2.map((_, s1))).flatMap {
              case (field, optSel) => 
                optSel.map((field, _))
            }
          }

          /**
           * All the relational operators require a field as one parameter, and 
           * BSON literal value as the other parameter. So we have to try to
           * extract out both a field annotation and a selector and then verify
           * the selector is actually a BSON literal value before we can 
           * construct the relational operator selector. If this fails for any
           * reason, it just means the given expression cannot be represented
           * using MongoDB's query operators, and must instead be written as
           * Javascript using the "$where" operator. Currently that's not supported.
           */
          def relop(f: Bson => Selector) = {
            extractFieldAndSelector.flatMap[Selector] {
              case (field, selector) => 
                selector match {
                  case Selector.Literal(bson) => Some(Selector.Doc(Map(field -> f(bson))))
                  case _ => None
                }
            }
          }
          def invoke1(f: Selector => Selector) = {
            val x :: Nil = args.map(_._3)

            x.map(f)
          }
          def invoke2Nel(f: NonEmptyList[Selector] => Selector) = {
            val x :: y :: Nil = args.map(_._3)

            (x |@| y)((a, b) => f(NonEmptyList(a, b)))
          }

          func match {
            case `Eq`       => relop(Selector.Eq.apply _)
            case `Neq`      => relop(Selector.Neq.apply _)
            case `Lt`       => relop(Selector.Lt.apply _)
            case `Lte`      => relop(Selector.Lte.apply _)
            case `Gt`       => relop(Selector.Gt.apply _)
            case `Gte`      => relop(Selector.Gte.apply _)

            case `And`      => invoke2Nel(Selector.And.apply _)
            case `Or`       => invoke2Nel(Selector.Or.apply _)
            case `Not`      => invoke1(Selector.Not.apply _)

            case `ObjectProject`  => promoteBsonField
            case `ArrayProject`   => promoteBsonField

            case _ => None
          }
        }

        node.fold[Output](
          read      = _ => promoteBsonField,
          constant  = data => Bson.fromData(data).fold[Output](
                        _ => None, 
                        d => Some(Selector.Literal(d))
                      ),
          join      = (_, _, _, _, _, _) => None,
          invoke    = invoke(_, _)
        )
      }
    })
  }

  private def getOrElse[A, B](b: B)(a: Option[A]): B \/ A = a.map(\/- apply).getOrElse(-\/ apply b)

  /**
   * In ANSI SQL, ORDER BY (AKA Sort) may operate either on fields derived in 
   * the query selection, or fields in the original data set.
   *
   * WHERE and GROUP BY (AKA Filter / GroupBy) may operate only on fields in the
   * original data set (or inline derivations thereof).
   *
   * Meanwhile, in a MongoDB pipeline, operators may only reference data in the 
   * original data set prior to a $project (PipelineOp.Project). All fields not
   * referenced in a $project are deleted from the pipeline.
   *
   * Further, MongoDB does not allow any field transformations in sorts or 
   * groupings.
   *
   * This means that we need to perform a LOT of pre-processing:
   *
   * 1. If WHERE or GROUPBY use inline transformations of original fields, then 
   *    these derivations have to be explicitly materialized as fields in the
   *    selection, and then these new fields used for the filtering / grouping.
   *
   * 2. Move Filter and GroupBy to just after the joins, so they can operate
   *    on the original data. These should be translated into MongoDB pipeline 
   *    operators ($match and $group, respectively).
   *
   * 3. For all original fields, we need to augment the selection with these
   *    original fields (using unique names), so they can survive after the
   *    projection, at which point we can insert a MongoDB Sort ($sort).
   *
   */

  /**
   * A helper function to determine if a plan consists solely of dereference 
   * operations. In MongoDB terminology, such a plan is referred to as a 
   * "field".
   */
  def justDerefs(t: Term[LogicalPlan2]): Boolean = {
    t.cata { (fa: LogicalPlan2[Boolean]) =>
      fa.fold(
        read      = _ => true,
        constant  = _ => false,
        join      = (_, _, _, _, _, _) => false,
        invoke    = (f, _) => f match {
          case `ObjectProject` => true
          case `ArrayProject` => true
          case _ => false
        }
      )
    }
  }

  /**
   * This helper function looks down the tree to discover Filter nodes.
   * When it finds one, it determines whether or not the Filter operates
   * on transformed fields. If so, it splits the filter into creation of
   * the transformed fields, and then filtering on the new fields.
   */
  def normalizeFilters(t: Term[LogicalPlan2]): Term[LogicalPlan2] = {
    t.topDownTransform { (t: Term[LogicalPlan2]) =>
      def unchanged1 = Function.const(t) _

      t.unFix.fold(
        read      = unchanged1,
        constant  = unchanged1,
        join      = (_, _, _, _, _, _) => t,
        invoke    = (f, _) => f match {
          case `Filter` => ???
          case _ => ???
        }
      )
    }
  }

  //def PreserveOriginalFields[A]: PhaseE[LogicalPlan2, PlannerError, A, A] = {
    // 
  //}

  /**
   * The pipeline phase tries to turn expressions and selectors into pipeline 
   * operations.
   *
   */
  def PipelinePhase: PhaseE[LogicalPlan2, PlannerError, (Option[Selector], Option[ExprOp]), List[PipelineOp]] = {
    type Input  = (Option[Selector], Option[ExprOp])
    type Output = PlannerError \/ List[PipelineOp]

    def nothing = \/- (Nil)

    def invoke(func: Func, args: List[(Term[LogicalPlan2], Input, Output)]): Output = {
      def selector(v: (Term[LogicalPlan2], Input, Output)): Option[Selector] = v._2._1

      def exprOp(v: (Term[LogicalPlan2], Input, Output)): Option[ExprOp] = v._2._2

      def pipelineOp(v: (Term[LogicalPlan2], Input, Output)): Option[List[PipelineOp]] = v._3.toOption

      def constant(v: (Term[LogicalPlan2], Input, Output)): Option[Data] = v._1.unFix.fold(
        read      = _ => None,
        constant  = Some(_),
        join      = (_, _, _, _, _, _) => None,
        invoke    = (_, _) => None
      )

      def constantStr(v: (Term[LogicalPlan2], Input, Output)): Option[String] = constant(v).collect {
        case Data.Str(text) => text
      }

      def constantLong(v: (Term[LogicalPlan2], Input, Output)): Option[Long] = constant(v).collect {
        case Data.Int(v) => v.toLong
      }

      def getOrFail[A](msg: String)(a: Option[A]): PlannerError \/ A = a.map(\/-.apply).getOrElse(-\/(PlannerError.InternalError(msg)))

      func match {
        case `MakeArray` => 
          getOrFail("Expected to find an expression for array argument")(args match {
            case value :: Nil =>
              for {
                value <- exprOp(value)
              } yield PipelineOp.Project(PipelineOp.Reshape(Map("0" -> -\/(value)))) :: Nil

            case _ => None
          })

        case `MakeObject` =>
          getOrFail("Expected to find string for field name and expression for field value")(args match {
            case field :: obj :: Nil =>
              for {
                field <- constantStr(field)
                obj   <- exprOp(obj)
              } yield PipelineOp.Project(PipelineOp.Reshape(Map(field -> -\/(obj)))) :: Nil

            case _ => None
          })
        
        case `ObjectConcat` => 
          getOrFail("Expected both left and right of object concat to be projection pipeline ops")(args match {
            case left :: right :: Nil =>
              for {
                left      <- pipelineOp(left)
                right     <- pipelineOp(right)
                leftMap   <- left.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
                rightMap  <- right.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
              } yield PipelineOp.Project(PipelineOp.Reshape(leftMap ++ rightMap)) :: (left.tail ++ right.tail)

            case _ => None
          })
        
        case `ArrayConcat` => 
          getOrFail("Expected both left and right of array concat to be projection pipeline ops")(args match {
            case left :: right :: Nil =>
              for {
                left      <- pipelineOp(left)
                right     <- pipelineOp(right)
                leftMap   <- left.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
                rightMap  <- right.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
              } yield PipelineOp.Project(PipelineOp.Reshape(leftMap ++ rightMap)) :: (left.tail ++ right.tail)

            case _ => None
          })

        case `Filter` =>           
          getOrFail("Expected pipeline op for set being filtered and selector for filter")(args match {
            case ops :: filter :: Nil => for {
              ops <- pipelineOp(ops)
              sel <- selector(filter) 
            } yield PipelineOp.Match(sel) :: ops

            case _ => None
          })

        case `Drop` =>
          getOrFail("Expected pipeline op for set being skipped and number to skip")(args match {
            case set :: count :: Nil => for {
              ops   <- pipelineOp(set)
              count <- constantLong(count)
            } yield PipelineOp.Skip(count) :: ops

            case _ => None
          })
        
        case `Take` =>
          getOrFail("Expected pipeline op for set being limited and number to limit")(args match {
            case set :: count :: Nil => for {
              ops   <- pipelineOp(set)
              count <- constantLong(count)
            } yield PipelineOp.Limit(count) :: ops

            case _ => None
          })

        case `GroupBy` => 
          \/-(PipelineOp.Group(???) :: Nil)

        case _ => nothing
      }
    }

    toPhaseE(Phase[LogicalPlan2, Input, Output] { (attr: LPAttr[Input]) =>
      scanPara2(attr) { (inattr: Input, node: LogicalPlan2[(Term[LogicalPlan2], Input, Output)]) =>
        node.fold[Output](
          read      = _ => ???,
          constant  = _ => ???,
          join      = (_, _, _, _, _, _) => ???,
          invoke    = invoke(_, _)
        )
      }
    })
  }

  def plan(logical: LPTerm, dest: String): PlannerError \/ Workflow = {
    ???
  }
}