package slamdata.engine.physical.mongodb

import slamdata.engine.fp._
import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._
import Liskov._

package object optimize {
  object pipeline {
    import ExprOp._
    import PipelineOp._

    type POptimizer = List[PipelineOp] => List[PipelineOp]

    private val IsProject: PartialFunction[PipelineOp, Project] = {
      case p @ Project(_) => p
    }

    private val IsGroup: PartialFunction[PipelineOp, Group] = {
      case g @ Group(_, _) => g
    }

    def zoptimizer(f: PipelineZipper[Option[PipelineOp]] => Option[PipelineZipper[Option[PipelineOp]]]): POptimizer = 
      list => f(PipelineZipper(list)).map(_.build).getOrElse(list)

    // This is a failed experiment on a zipper with a polymorphic hole. Used by find methods but no where else,
    // needs refactoring so that current is always right.headOption, will clean up a lot of code.
    case class PipelineZipper[A] private (left: List[PipelineOp], cursor: A, right: List[PipelineOp]) {
      type Possibly[A] = PipelineZipper[Option[A]]

      def size(implicit ev: A <~< Option[PipelineOp]) = left.length + ev(cursor).toList.length + right.length

      def build(implicit ev: A <~< Option[PipelineOp]): List[PipelineOp] = 
        left.reverse ::: ev(cursor).toList ::: right

      def some: PipelineZipper[Option[A]] = map(Some(_))

      def get: A = cursor

      def atStart(implicit ev: A <~< Option[PipelineOp]): Boolean = index == 0

      def atEnd(implicit ev: A <~< Option[PipelineOp]): Boolean = index >= size

      def map[B](f: A => B): PipelineZipper[B] = set(f(get))

      def set[B](v: B): PipelineZipper[B] = copy(cursor = v)

      def index: Int = left.length

      def convert(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = set(ev(get))

      def prev(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = 
        left.headOption.map[Possibly[PipelineOp]](h => copy(left = left.tail, cursor = Some(h), right = ev(cursor).toList ::: right)).getOrElse(convert.pushRight)

      def peekPrev: Option[PipelineOp] = left.headOption

      def next(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        right.headOption.map[Possibly[PipelineOp]] { h => 
          copy(left = ev(cursor).toList ::: left, cursor = Some(h), right = right.tail)
        }.getOrElse(convert.pushLeft)
      }

      def peekNext: Option[PipelineOp] = right.headOption

      def prevSchema: SchemaChange = left.foldLeft[SchemaChange](SchemaChange.Init) {
        case (schema, op) => schema.rebase(op.schema)
      }

      def curSchema(implicit ev: A <~< Option[PipelineOp]): SchemaChange = 
        ev(cursor).map(_.schema.rebase(prevSchema)).getOrElse(prevSchema)

      def findFirst[B](f: PartialFunction[PipelineOp, B])(implicit ev: A <~< Option[PipelineOp]): Possibly[B] = {
        if (atEnd) convert.pushLeft.set[Option[B]](None)
        else ev(cursor).flatMap(f.lift).map(b => set[Option[B]](Some(b))).getOrElse(convert.next.findFirst(f))
      }

      def findNext[B](f: PartialFunction[PipelineOp, B])(implicit ev: A <~< Option[PipelineOp]): Possibly[B] = 
        convert.next.findFirst(f)

      def goto(index0: Int)(implicit ev: A <~< Option[PipelineOp]): Option[Possibly[PipelineOp]] = {
        if (index0 < 0) None
        else if (index0 >= size) None
        else if (index0 == this.index) Some(set(ev(cursor)))
        else if (index0 < this.index) convert.prev.goto(index0)
        else if (index0 > this.index) convert.next.goto(index0)
        else None
      }

      def first(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = 
        if (left == Nil) convert
        else prev.first

      def last(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = 
        if (right == Nil) convert
        else next.last

      def delete: Possibly[PipelineOp] = right match {
        case Nil => set(None)
        case x :: xs => copy(cursor = Some(x), right = xs)
      }

      def pushLeft(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        copy(left = ev(cursor).toList ::: left, cursor = None)
      }

      def pushRight(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        copy(right = ev(cursor).toList ::: right, cursor = None)
      }

      def between(start: Int, end: Int)(implicit ev: A <~< Option[PipelineOp]): List[PipelineOp] = 
        build.drop(start).take(end - start)

      def skipForward(num: Int)(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        if (num <= 0) convert
        else convert.next.skipForward(num - 1)
      }

      def take(num: Int)(implicit ev: A <~< Option[PipelineOp]): List[PipelineOp] = {
        convert.build.drop(index).take(num)
      }

      def delete(num: Int)(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        val b = convert.build
        
        val prefix = b.take(index).reverse
        val suffix = b.drop(index + num)

        PipelineZipper(prefix, suffix.headOption, suffix.headOption.map(_ => suffix.tail).getOrElse(Nil))
      }

      def insert(ops: List[PipelineOp]): PipelineZipper[A] = copy(right = ops ::: right)

      def normalize(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        val c = convert

        c.cursor match {
          case Some(head) => c
          case None => c.copy(cursor = right.headOption, right = right.headOption.map(_ => right.tail).getOrElse(Nil))
        }
      }

      def filter(f: PipelineOp => Boolean)(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = {
        val n = normalize 

        if (n.atEnd) n else n.set(n.cursor.filter(f)).next.filter(f)
      }

      def mapState[Z](z: Z)(f: (Z, PipelineOp) => (Z, PipelineOp))(implicit ev: A <~< Option[PipelineOp]): Possibly[PipelineOp] = 
        mapState0(z)(f)._2

      def mapState0[Z](z: Z)(f: (Z, PipelineOp) => (Z, PipelineOp))(implicit ev: A <~< Option[PipelineOp]): (Z, Possibly[PipelineOp]) = {
        val n = normalize

        (n.cursor.map { head =>
          val (z2, head2) = f(z, head)

          n.set(Some(head2)).next.mapState0(z2)(f)
        }).getOrElse(z -> n)
      }

      def edit(startIdx: Int, endIdx: Int)(f: Possibly[PipelineOp] => Option[Possibly[PipelineOp]])(implicit ev: A <~< Option[PipelineOp]): Option[Possibly[PipelineOp]] = {
        val c = convert

        val b = convert.build

        val prefix = b.take(startIdx)
        val middle = b.drop(startIdx).take(endIdx - startIdx)
        val suffix = b.drop(endIdx)

        for {
          middle <- f(PipelineZipper(middle)).map(_.build)
          rez    <- PipelineZipper(prefix ::: middle ::: suffix).goto(index)
        } yield rez
      }
    }

    object PipelineZipper {
      def empty[A] = PipelineZipper[Option[A]](Nil, None, Nil)

      def apply(list: List[PipelineOp]): PipelineZipper[Option[PipelineOp]] = list match {
        case x :: xs => new PipelineZipper[Option[PipelineOp]](Nil, Some(x), xs)
        case Nil => empty
      }
    }

    val deleteUnusedFields: POptimizer = zoptimizer { zipper =>
      def toOp[A <: PipelineOp, B <: PipelineOp](e: A \/ B): PipelineOp = e.fold(a => a: PipelineOp, a => a: PipelineOp)

      def optimize(zipper: PipelineZipper[Option[PipelineOp]]): Option[PipelineZipper[Option[PipelineOp]]] = {
        if (zipper.atEnd) None
        else {
          val start   = zipper.findFirst(IsProject |?| IsGroup)
          val startOp = start.map(_.map(toOp))
          val end     = startOp.findNext(IsProject |?| IsGroup)

          if (end.map(_.map(toOp)).atEnd) None
          else {
            //println("start.index = " + start.index)
            //println("end.index   = " + end.index)

            def getDefs(e: Project \/ Group): Set[DocVar] = 
              (e.fold(_.getAll.map(_._1), _.getAll.map(_._1)).map(DocVar.ROOT(_))).toSet

            def getRefs(op: PipelineOp): Set[DocVar] = (op match {
              case Unwind(_) => Nil // Don't count unwinds (if the var isn't referenced elsewhere, it's effectively unused)
              case _ => op.refs
            }).toSet

            def unused(defs: Set[DocVar], refs: Set[DocVar]): Set[DocVar] = {
              defs.filterNot(d => refs.exists(ref => d.startsWith(ref) || ref.startsWith(d)))
            }

            val between = zipper.between(start.index + 1, end.index + 1).toSet

            val usedRefs = between.foldLeft(List.empty[DocVar])(_ ++ getRefs(_)).toSet

            start.get.flatMap { start0 =>
              val unusedRefs = unused(getDefs(start0), usedRefs).toList.flatMap(_.deref.toList)

              //println("defs = " + getDefs(start0))
              //println("used refs = " + usedRefs)
              //println("unused refs = " + unusedRefs)

              (start0.fold[PipelineZipper[Option[PipelineOp]]](
                project => start.set(Some(project.deleteAll(unusedRefs))),
                group   => start.set(Some(group.deleteAll(unusedRefs.map(_.flatten.head))))
              )).edit(start.index + 1, end.index) { zipper =>
                Some(zipper.filter { 
                  case Unwind(ref) => !unusedRefs.contains(ref)
                  case _ => true 
                })
              }.map(_.next)

              //println(end.build)
            }
          }
        }
      }

      def loop(zipper: PipelineZipper[Option[PipelineOp]]): Option[PipelineZipper[Option[PipelineOp]]] = {
        optimize(zipper).map { rest =>
          // Optimize will return None when the required structure can't be found.
          loop(rest).getOrElse(rest)
        }  
      }

      loop(zipper)
    }

    def mergeAdjacent(fst: Project, snd: Project): Option[Project] = inlineProject(snd.shape, fst.shape :: Nil).map(Project(_))

    def get0(leaves: List[BsonField.Leaf], rs: List[Reshape]): Option[ExprOp \/ Reshape] = {
      (leaves, rs) match {
        case (_, Nil) => Some(-\/ (BsonField(leaves).map(DocVar.ROOT(_)).getOrElse(DocVar.ROOT())))

        case (Nil, r :: rs) => inlineProject(r, rs).map(\/- apply)

        case (l :: ls, r :: rs) => r.get(l).flatMap {
          case -\/ (d @ DocVar(_, _)) => get0(d.path ++ ls, rs)

          case -\/ (e) => 
            if (ls.isEmpty) fixExpr(rs, e).map(-\/ apply) else None

          case \/- (r) => get0(ls, r :: rs)
        }
      }
    }

    private def fixExpr(rs: List[Reshape], e: ExprOp): Option[ExprOp] = {
      type OptionTramp[X] = OptionT[Free.Trampoline, X]

      def lift[A](o: Option[A]): OptionTramp[A] = OptionT(o.point[Free.Trampoline])

      (e.mapUpM[OptionTramp] {
        case ref @ DocVar(_, _) => 
          lift {
            get0(ref.path, rs).flatMap(_.fold(Some.apply, _ => None))
          }
      }).run.run
    }

    private def inlineProject(r: Reshape, rs: List[Reshape]): Option[Reshape] = {
      type MapField[X] = Map[BsonField, X]

      val p = Project(r)

      val map = Traverse[MapField].sequence(p.getAll.toMap.mapValues {
        case d @ DocVar(_, _) => get0(d.path, rs)
        case e => fixExpr(rs, e).map(-\/ apply)
      })

      map.map(vs => p.empty.setAll(vs).shape)
    }

    private def inlineGroupProjects(g: Group, ps: List[Project]): Option[Group] = {
      import ExprOp._

      val rs = ps.map(_.shape)

      type MapField[X] = ListMap[BsonField.Leaf, X]

      val grouped = Traverse[MapField].sequence(ListMap(g.getAll: _*).map { t =>
        val (k, v) = t

        k -> (v match {
          case AddToSet(e)  =>
            fixExpr(rs, e) flatMap {
              case d @ DocVar(_, _) => Some(First(d))
              case _ => None
            }
          case Push(e)      =>
            fixExpr(rs, e) flatMap {
              case d @ DocVar(_, _) => Some(Push(d))
              case _ => None
            }
          case First(e)     => fixExpr(rs, e).map(First(_))
          case Last(e)      => fixExpr(rs, e).map(Last(_))
          case Max(e)       => fixExpr(rs, e).map(Max(_))
          case Min(e)       => fixExpr(rs, e).map(Min(_))
          case Avg(e)       => fixExpr(rs, e).map(Avg(_))
          case Sum(e)       => fixExpr(rs, e).map(Sum(_))
        })
      })

      val by = g.by.fold(e => fixExpr(rs, e).map(-\/ apply), r => inlineProject(r, rs).map(\/- apply))

      (grouped |@| by)((grouped, by) => Group(Grouped(grouped), by))
    }

    val coalesceProjects: POptimizer = (p: List[PipelineOp]) => spansOpt(p)({
      case p @ Project(_) => p
    })(ps0 => {
      val rs = ps0.reverse.map(_.shape)
      
      inlineProject(rs.head, rs.tail).map(p => Project(p) :: Nil)
    }, ops => Some(ops.list)).getOrElse(p)

    val coalesceGroupProjects: POptimizer = (ps: List[PipelineOp]) => {
      val groups = ps.zipWithIndex.collect {
        case x @ (Group(_, _), _) => x
      }

      val groupIndices = groups.map(_._2)

      val groupSpans = groupIndices.lastOption.map { last =>
        (0 :: groupIndices.map(_ + 1)).zip(groupIndices) ::: (if (last == ps.length - 1) Nil else (last + 1, ps.length - 1) :: Nil)
      }

      groupSpans.map { bounds =>
        bounds.foldLeft(List.empty[PipelineOp]) {
          case (acc, (start, end)) =>
            val before = ps.drop(start).take(end - start).reverse
            val at     = ps(end)

            val (projects, others) = collectWhile(before) {
              case (p @ Project(_)) => p
            }

            (at match {
              case g @ Group(_, _) => inlineGroupProjects(g, projects).map(_ :: others)

              case _ => None
            }).getOrElse(at :: before) ::: acc
        }.reverse
      }.getOrElse(ps)
    }

    val simplify: POptimizer = (coalesceProjects andThen coalesceGroupProjects) andThen deleteUnusedFields
  }
}