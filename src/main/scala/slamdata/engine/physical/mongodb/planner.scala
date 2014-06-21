package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._

import scalaz.{Free => FreeM, Node => _, _}
import scalaz.task.Task

import scalaz.syntax.either._
import scalaz.syntax.compose._
import scalaz.syntax.applicativePlus._

import scalaz.std.AllInstances._

object MongoDbPlanner extends Planner[Workflow] {
  import LogicalPlan._

  import slamdata.engine.analysis.fixplate._

  import set._
  import relations._
  import structural._
  import math._
  import agg._

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
   * operations (or worse): [dereference, middle op, dereference].
   */
  def FieldPhase[A]: PhaseE[LogicalPlan, PlannerError, A, Option[BsonField]] = lpBoundPhaseE {
    type Output = Option[BsonField]
    
    liftPhaseE(Phase { (attr: LPAttr[A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[(LPTerm, Output)]) =>
        node.fold[Output](
          read      = Function.const(None), 
          constant  = Function.const(None),
          join      = (left, right, tpe, rel, lproj, rproj) => None,
          invoke    = (func, args) => 
                      if (func == ObjectProject) {
                        // TODO: Make pattern matching safer (i.e. generate error if pattern not matched):
                        val (objTerm, objAttrOpt) :: (Term(LogicalPlan.Constant(Data.Str(fieldName))), _) :: Nil = args

                        Some(objAttrOpt match {
                          case Some(objAttr) => objAttr \ BsonField.Name(fieldName)

                          case None => BsonField.Name(fieldName)
                        })
                      } else if (func == ArrayProject) {
                        // Mongo treats array derefs the same as object derefs.
                        val (objTerm, objAttrOpt) :: (Term(LogicalPlan.Constant(Data.Int(index))), _) :: Nil = args

                        Some(objAttrOpt match {
                          case Some(objAttr) => objAttr \ BsonField.Name(index.toString)

                          case None => BsonField.Name(index.toString)
                        })
                      } else {
                        None
                      },
          free      = Function.const(None),
          let       = (let, in) => None // ???
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
  def ExprPhase: PhaseE[LogicalPlan, PlannerError, Option[BsonField], Option[ExprOp]] = lpBoundPhaseE {
    type Output = PlannerError \/ Option[ExprOp]

    toPhaseE(Phase { (attr: LPAttr[Option[BsonField]]) =>
      scanCata(attr) { (fieldAttr: Option[BsonField], node: LogicalPlan[Output]) =>
        def emit(expr: ExprOp): Output = \/- (Some(expr))

        // Promote a bson field annotation to an expr op:
        def promoteField = \/- (fieldAttr.map(ExprOp.DocField.apply _))

        def nothing = \/- (None)

        def invoke(func: Func, args: List[Output]): Output = {
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

            case `Count`    => emit(ExprOp.Count)
            case `Sum`      => invoke1(ExprOp.Sum.apply _)
            case `Avg`      => invoke1(ExprOp.Avg.apply _)
            case `Min`      => invoke1(ExprOp.Min.apply _)
            case `Max`      => invoke1(ExprOp.Max.apply _)

            case `ObjectProject`  => promoteField
            case `ArrayProject`   => promoteField

            case _ => nothing
          }
        }

        node.fold[Output](
          read      = name => emit(ExprOp.DocVar.ROOT()),
          constant  = data => Bson.fromData(data).bimap[PlannerError, Option[ExprOp]](
                        _ => PlannerError.NonRepresentableData(data), 
                        d => Some(ExprOp.Literal(d))
                      ),
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _),
          free      = _ => nothing,
          let       = (_, in) => in
        )
      }
    })
  }

  private type EitherPlannerError[A] = PlannerError \/ A

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
   * factoring out the leftovers for conversion using $where.
   *
   */
  def SelectorPhase: PhaseE[LogicalPlan, PlannerError, Option[BsonField], Option[Selector]] = lpBoundPhaseE {
    type Input = Option[BsonField]
    type Output = Option[Selector]

    liftPhaseE(Phase { (attr: LPAttr[Input]) =>
      scanPara2(attr) { (fieldAttr: Input, node: LogicalPlan[(Term[LogicalPlan], Input, Output)]) =>
        def emit(sel: Selector): Output = Some(sel)

        def invoke(func: Func, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
 
          def extractValue(t: Term[LogicalPlan]): Option[Bson] =
            t.unFix.fold(
              read      = _ => None,
              constant  = data => Bson.fromData(data).toOption,
              join      = (_, _, _, _, _, _) => None,
              invoke    = (_, _) => None,
              free      = _ => None,
              let       = (_, _) => None
            )
           
          /**
           * Attempts to extract a BsonField annotation and a Bson value from
           * an argument list of length two (in any order).
           */
          def extractFieldAndSelector: Option[(BsonField, Bson)] = {
            val (t1, f1, _) :: (t2, f2, _) :: Nil = args
            
            val v1 = extractValue(t1)
            val v2 = extractValue(t2)

            f1.map((_, v2)).orElse(f2.map((_, v1))).flatMap {
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
           * Javascript using the "$where" operator.
           */
          def relop(f: Bson => Selector.Condition) =
            for {
              (field, value) <- extractFieldAndSelector
            } yield Selector.Doc(Map(field -> Selector.Expr(f(value))))

          def invoke2Nel(f: (Selector, Selector) => Selector) = {
            val x :: y :: Nil = args.map(_._3)

            (x |@| y)(f)
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
            // case `Not`      => invoke1(Selector.Not.apply _)

            case _ => None
          }
        }

        node.fold[Output](
          read      = _ => None,
          constant  = _ => None,
          join      = (_, _, _, _, _, _) => None,
          invoke    = invoke(_, _),
          free      = _ => None,
          let       = (_, in) => in._3
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
   * HAVING may operate on fields in the original data set or fields in the selection.
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
  def justDerefs(t: Term[LogicalPlan]): Boolean = {
    t.cata { (fa: LogicalPlan[Boolean]) =>
      fa.fold(
        read      = _ => true,
        constant  = _ => false,
        join      = (_, _, _, _, _, _) => false,
        invoke    = (f, _) => f match {
          case `ObjectProject` => true
          case `ArrayProject` => true
          case _ => false
        },
        free      = _ => false,
        let       = (_, _) => false
      )
    }
  }

  /**
   * The pipeline phase tries to turn expressions and selectors into pipeline 
   * operations.
   *
   */
  def PipelinePhase: PhaseE[LogicalPlan, PlannerError, (Option[Selector], Option[ExprOp]), List[PipelineOp]] = lpBoundPhaseE {
    /*
      Notes on new approach:
      
      1. If this node is annotated with an ExprOp, DON'T DO ANYTHING.
      2. If this node is NOT annotated with an ExprOp, we need to try to create a Pipeline.
          a. If the children have Pipelines, then use those to form the new pipeline in a function-specific fashion.
          b. If the children don't have Pipelines, try to promote them to pipelines in a function-specific manner,
             then use those pipelines to form the new pipeline in a function-specific manner.
          c. If the node cannot be converted to a Pipeline, the process ends here.

      
  
    */
    type Input  = (Option[Selector], Option[ExprOp])
    type Output = PlannerError \/ List[PipelineOp]

    def nothing = \/- (Nil)

    def invoke(func: Func, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
      def merge(left: List[PipelineOp], right: List[PipelineOp]): PlannerError \/ List[PipelineOp] = 
        if (left != right) -\/ (PlannerError.InternalError("Diverging pipeline histories: " + left + ", " + right))
        else \/- (left)

      def selector(v: (Term[LogicalPlan], Input, Output)): Option[Selector] = (v _2) _1

      def exprOp(v: (Term[LogicalPlan], Input, Output)): Option[ExprOp] = (v _2) _2

      def pipelineOp(v: (Term[LogicalPlan], Input, Output)): Option[List[PipelineOp]] = (v _3) toOption

      def constant(v: (Term[LogicalPlan], Input, Output)): Option[Data] = v._1.unFix.fold(
        read      = _ => None,
        constant  = Some(_),
        join      = (_, _, _, _, _, _) => None,
        invoke    = (_, _) => None,
        free      = _ => None,
        let       = (_, _) => None
      )

      def constantStr(v: (Term[LogicalPlan], Input, Output)): Option[String] = constant(v) collect {
        case Data.Str(text) => text
      }

      def constantLong(v: (Term[LogicalPlan], Input, Output)): Option[Long] = constant(v) collect {
        case Data.Int(v) => v.toLong
      }

      def getOrFail[A](msg: String)(a: Option[A]): PlannerError \/ A = a.map(\/-.apply).getOrElse(-\/(PlannerError.InternalError(msg + ": " + args)))

      func match {
        case `MakeArray` => 
          getOrFail("Expected to find an expression for array argument")(args match {
            case value :: Nil =>
              for {
                // FIXME: pipelineOps on value
                value <- exprOp(value)
              } yield PipelineOp.Project(PipelineOp.Reshape(Map(BsonField.Name("0") -> -\/(value)))) :: Nil

            case _ => None
          })
          
        case `ObjectProject` => 
          getOrFail("Expected nothing in particluar for projection")(args match {
            case obj :: field :: Nil => pipelineOp(obj) // Propagate pipeline ops attached to "obj"
            case _ => None
          })

        case `MakeObject` =>
          getOrFail("Expected to find string for field name and expression for field value")(args match {
            case field :: obj :: Nil =>
              for {
                ops   <- pipelineOp(obj) // FIXME
                field <- constantStr(field)
                obj   <- exprOp(obj)
              } yield PipelineOp.Project(PipelineOp.Reshape(Map(BsonField.Name(field) -> -\/(obj)))) :: ops

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
                
                val ltail = left.tailOption.getOrElse(Nil)
                val rtail = right.tailOption.getOrElse(Nil)

                tail      <- merge(ltail, rtail).toOption // FIXME
              } yield PipelineOp.Project(PipelineOp.Reshape(leftMap ++ rightMap)) :: tail

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
              } yield PipelineOp.Project(PipelineOp.Reshape(leftMap ++ rightMap)) :: (left.tail ++ right.tail) // TODO: Verify tails are empty

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
          type MapField[V] = Map[BsonField.Name, V]

          // MongoDB's $group cannot produce nested documents. So passes are required to support them.
          getOrFail("Expected (flat) projection pipeline op for set being grouped and expression op for value to group on")(args match {
            case set :: by :: Nil => for {
              ops     <-  pipelineOp(set) // TODO: Tail
              reshape <-  ops.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
              grouped <-  Traverse[MapField].sequence(reshape.mapValues { 
                            case -\/(groupOp : ExprOp.GroupOp) => Some(groupOp)

                            case _ => None 
                          })
              by      <-  exprOp(by)
            } yield PipelineOp.Group(PipelineOp.Grouped(grouped), by) :: ops.tailOption.getOrElse(Nil)

            case _ => None
          })

        case `OrderBy` => {
          def invoke(v: (Term[LogicalPlan], Input, Output)): Option[(Func, List[Term[LogicalPlan]])] = v._1.unFix.fold(
            read      = _ => None,
            constant  = _ => None,
            join      = (_, _, _, _, _, _) => None,
            invoke    = (func, args) => Some(func -> args),
            free      = _ => None,
            let       = (_, _) => None
          )
        
          def invokeProject(v: (Term[LogicalPlan], Input, Output)): Option[(Term[LogicalPlan], String)] = for {
            (`ObjectProject`, args) <- invoke(v)
            val obj = args(0)
            name <- constantStr(args(1), v._2, v._3)  // TODO: need to do something with in/out?
          } yield (obj, name)
          
          def invokeProjectArray(v: (Term[LogicalPlan], Input, Output)): Option[String] = for {
            (`MakeArray`, args) <- invoke(v)
            (obj, name) <- invokeProject(args(0), v._2, v._3)  // TODO: need to do something with in/out?
          } yield name
          
          def invokeProjectArrayConcat(v: (Term[LogicalPlan], Input, Output)): Option[NonEmptyList[String]] = {
            import Scalaz._
            val namesFromArray: Option[NonEmptyList[String]] = for {
              (`ArrayConcat`, args) <- invoke(v)
              names <- args.map(invokeProjectArrayConcat(_, v._2, v._3)).sequence  // TODO: need to do something with in/out?
              val flatNames = names.flatMap(_.list)
            } yield NonEmptyList.nel(flatNames.head, flatNames.tail)
            namesFromArray.orElse(invokeProjectArray(v).map(NonEmptyList(_)))
          }
          
          getOrFail("Expected pipeline op for set being sorted and keys")(args match {
            case set :: keys :: Nil => for {
              ops <- pipelineOp(set)
              keyNames <- invokeProjectArrayConcat(keys)
              val sortType: SortType = Ascending  // TODO: asc vs. desc
            } yield PipelineOp.Sort(keyNames.map(n => (BsonField.Name(n) -> sortType))) :: ops
            
            case _ => None
          })
        }
          
        
        case _ => nothing
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: LPAttr[Input]) =>
      // println(Show[Attr[LogicalPlan, Input]].show(attr).toString)

      scanPara2(attr) { (inattr: Input, node: LogicalPlan[(Term[LogicalPlan], Input, Output)]) =>
        node.fold[Output](
          read      = _ => nothing,
          constant  = _ => nothing,
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _),
          free      = _ => nothing, // ???
          let       = (let, in) => in._3
        )
      }
    })
  }

  /**
   * A workflow build represents a partially-constructed workflow.
   *
   * It consists of a portion that's done (but possibly not up-to-date), as 
   * well as a portion that is staged (and up-to-date) but may ultimately be 
   * replaced with something else higher up the tree.
   *
   * This is used for the workflow phase.
   */
  case class WorkflowBuild private (state: Option[(WorkflowTask, Option[WorkflowTask])]) {
    def done = state.map(_._1)

    def draft = state.flatMap(_._2)

    def accept: WorkflowBuild = WorkflowBuild(finish.map(_ -> None))

    def stage(f: WorkflowTask => WorkflowTask): WorkflowBuild = WorkflowBuild(done.map(done => done -> Some(f(done))))

    def finish: Option[WorkflowTask] = draft.orElse(done)
  }

  object WorkflowBuild {
    import WorkflowTask._

    val Empty = new WorkflowBuild(None)

    def done(task: WorkflowTask) = new WorkflowBuild(Some(task -> None))

    implicit val WorkflowBuildMonoid = new Monoid[WorkflowBuild] {
      def zero = Empty

      def append(v1: WorkflowBuild, v2: => WorkflowBuild): WorkflowBuild = {
        val done = 
          (v1.done |@| v2.done) { 
            case (v1, v2) if v1 == v2 => v1
            case (v1, v2) => JoinTask(NonEmptyList(v1, v2)) 
          } orElse
          (v1.done) orElse
          (v2.done)

        val draft = (v1.draft |@| v2.draft)((v1, v2) => JoinTask(NonEmptyList(v1, v2))) orElse 
          (v1.draft) orElse
          (v2.draft)

        new WorkflowBuild(done.map(done => done -> draft))
      }
    }

    implicit val WorkflowBuildShow = new Show[WorkflowBuild] {
      override def show(v: WorkflowBuild): Cord = Cord(v.toString) // TODO
    }
  }

  /**
   * The workflow phase builds on pipeline operations, turning them into workflow tasks.
   */
  def WorkflowPhase: PhaseE[LogicalPlan, PlannerError, List[PipelineOp], WorkflowBuild] = lpBoundPhaseE {
    import WorkflowTask._

    type Input  = List[PipelineOp]
    type Output = PlannerError \/ WorkflowBuild

    def merge(xs: List[WorkflowBuild]) = Foldable[List].foldMap(xs toList)(identity)

    def nothing = \/- (WorkflowBuild.Empty)

    def emit[A](a: A): PlannerError \/ A = \/-(a)

    def combine(ops: Input, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
      val build = Traverse[List].sequenceU(args.map(_._3)).map(merge _)
      val ops2 = ops.reverse  // ops list built in reverse order in pipeline phase
      build.map(_.stage(parent => WorkflowTask.PipelineTask(parent, Pipeline(ops2))))
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: LPAttr[Input]) =>
      // println(Show[Attr[LogicalPlan, Input]].show(attr).toString)

      scanPara2(attr) { (inattr: Input, node: LogicalPlan[(Term[LogicalPlan], Input, Output)]) =>
        node.fold[Output](
          read      = name => \/- (WorkflowBuild.done(WorkflowTask.ReadTask(Collection(name.filename)))),
          constant  = _ => nothing,
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = (f, args) => combine(inattr, args),
          free      = _ => nothing,
          let       = (_, in) => combine(inattr, in :: Nil)
        )
      }
    })
  }

  val AllPhases = (FieldPhase[Unit]).fork(SelectorPhase, ExprPhase) >>> PipelinePhase >>> WorkflowPhase

  def plan(logical: Term[LogicalPlan]): PlannerError \/ Workflow = {
    import WorkflowTask._

    val workflowBuild = AllPhases(attrUnit(logical)).map(_.unFix.attr)

    workflowBuild.flatMap { build =>
      build.finish match {
        case Some(task) => 

          \/- (Workflow(task))

        case None => -\/ (PlannerError.InternalError("The plan cannot yet be compiled to a MongoDB workflow"))
      }
    }
  }
}
