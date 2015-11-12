package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.{NameGenerator => QNameGenerator}
import quasar.javascript._
import quasar.physical.mongodb.workflowtask._

import scalaz._, Scalaz._

/** Implements the necessary operations for executing a `Workflow` against
  * MongoDB.
  */
trait WorkflowExecutor[F[_]] {
  /** Execute the given aggregation pipeline with the given collection as
    * input.
    */
  def aggregate(src: Collection, pipeline: Pipeline): F[Unit]

  /** Drop the given collection. */
  def drop(coll: Collection): F[Unit]

  /** Insert the given BSON documents into the specified collection. */
  def insert(dst: Collection, values: List[Bson.Doc]): F[Unit]

  /** Execute the given MapReduce job, sourcing values from the given `src`
    * collection and writing the results to the destination collection.
    */
  def mapReduce(src: Collection, dstCollectionName: String, mr: MapReduce): F[Unit]

  /** Rename the `src` collection to `dst` collection, overwriting it if it
    * exists.
    */
  def rename(src: Collection, dst: Collection): F[Unit]

  //--- Derived Methods ---

  type G0[A]  = SeqNameGeneratorT[F, A]
  type G[A]   = ReaderT[G0, String, A]
  type E[A,B] = EitherT[G, A, B]
  type M[A]   = WorkflowExecErrT[G, A]

  import WorkflowExecutionError._
  import Workflow._

  /** Returns the `Collection` containing the results of executing the given
    * (crystallized) `Workflow`.
    */
  def execute(workflow: Crystallized, dst: Collection)
             (implicit F: Monad[F])
             : M[Collection] = {

    // The set of temp collections written to in the computation
    type Temps           = Set[Collection]
    type TempsT[X[_], A] = StateT[X, Temps, A]
    type N[A]            = TempsT[M, A]

    // NB: This handholding was necessary to resolve "divering implicit
    //     expansion" errors for Monad[M] when attempting to summon Monad[N].
    implicit val N: Monad[N] = StateT.stateTMonadState[Temps, M](Monad[M])

    def wroteTo(c: Collection, didWrite: Boolean): N[Unit] =
      Lens.setMembershipLens(c).assign(didWrite).void.lift[M]

    def tempColl: N[Collection] =
      for {
        tmp <- NameGenerator[M].prefixedName("wf.").liftM[TempsT]
        col =  Collection(dst.databaseName, tmp)
        _   <- wroteTo(col, true) // assume we'll write to this collection
      } yield col

    def unableToStore[A](bson: Bson): N[A] =
      InsertFailed(
        bson,
        s"MongoDB is only able to store documents in collections, not `$bson`."
      ).raiseError[E, A].liftM[TempsT]

    def liftFM[A](fa: F[A]): M[A] =
      (fa.liftM[SeqNameGeneratorT]
        .liftM[ReaderT[?[_], String, ?]]: G[A])
        .liftM[WorkflowExecErrT]

    def execute0(wt: WorkflowTask, out: Collection): N[Collection] = wt match {
      case PureTask(doc @ Bson.Doc(_)) =>
        liftFM(insert(out, List(doc)))
          .liftM[TempsT]
          .as(out)

      case PureTask(Bson.Arr(vs)) =>
        for {
          docs <- vs.toList.traverseU {
                    case doc @ Bson.Doc(_) => doc.right
                    case other             => other.left
                  } fold (unableToStore[List[Bson.Doc]], _.point[N])
          _    <- liftFM(insert(out, docs)).liftM[TempsT]
        } yield out

      case PureTask(v) =>
        unableToStore(v)

      case ReadTask(coll) =>
        // Update the state to reflect that `out` wasn't used.
        wroteTo(out, false) as coll

      case QueryTask(source, query, skip, limit) =>
        val pipelineQuery =
          List($Match((), query.query)) :::
          skip.map($Skip((), _)).toList :::
          limit.map($Limit((), _)).toList

        execute0(PipelineTask(source, pipelineQuery), out)

      case PipelineTask(source, pipeline) =>
        for {
          tmp <- tempColl
          src <- execute0(source, tmp)
          _   <- liftFM(aggregate(src, pipeline ::: List($Out((), out))))
                   .liftM[TempsT]
        } yield out

      // TODO: Should we extract the db, if any, from `mr`?
      case MapReduceTask(source, mr) =>
        for {
          tmp <- tempColl
          src <- execute0(source, tmp)
          _   <- liftFM(mapReduce(src, out.collectionName, mr))
                   .liftM[TempsT]
        } yield out

      case FoldLeftTask(rd @ ReadTask(_), _) =>
        (InvalidTask(rd, "FoldLeft from simple read")
          .raiseError[E, Collection]: M[Collection])
          .liftM[TempsT]

      case FoldLeftTask(head, tail) =>
        for {
          h <- execute0(head, out)
          _ <- tail.traverse_[N] { case MapReduceTask(source, mr) =>
                 tempColl flatMap (execute0(source, _)) flatMap { src =>
                   liftFM(mapReduce(src, h.collectionName, mr)).liftM[TempsT]
                 }
               }
        } yield h
    }

    execute0(Workflow task workflow, dst).run(Set()) flatMap { case (tmps, coll) =>
      tmps filter (_ != coll) traverse_ (c => liftFM(drop(c))) as coll
    }
  }
}

object WorkflowExecutor {
  import Workflow.Crystallized

  /** A `WorkflowExecutor` that executes a `Workflow` in the `MongoDb` monad. */
  val mongoDb: EnvErr2T[MongoDb, WorkflowExecutor[MongoDb]] = {
    import MongoDbWorkflowExecutor._

    type E[A, B] = EitherT[MongoDb, A, B]
    type M[A]    = EnvErr2T[MongoDb, A]

    liftEnvErr(MongoDb.serverVersion) flatMap { v =>
      if (v >= MinMongoDbVersion)
        (new MongoDbWorkflowExecutor: WorkflowExecutor[MongoDb]).point[M]
      else
        UnsupportedVersion2("MongoDB", v).raiseError[E, WorkflowExecutor[MongoDb]]
    }
  }

  /** A 'WorkflowExecutor` that interprets a `Workflow` in JavaScript. */
  val javaScript: WorkflowExecutor[JavaScriptLog] =
    new JavaScriptWorkflowExecutor

  /** Interpret a `Workflow` into an equivalent JavaScript program. */
  def toJS(workflow: Crystallized, dst: Collection): WorkflowExecutionError \/ String = {
    import Js._
    import JavaScriptWorkflowExecutor._

    javaScript.execute(workflow, dst).run.run("tmp.gen").eval(0).run flatMap {
      case (log, \/-(coll)) =>
        Stmts((log :+ Call(Select(toJsRef(coll), "find"), Nil)).toList)
          .pprint(0)
          .right

      case (_, -\/(err)) =>
        err.left
    }
  }
}
