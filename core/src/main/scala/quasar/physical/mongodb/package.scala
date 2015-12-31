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

package quasar.physical

import quasar.Predef.Vector
import quasar.javascript.Js
import quasar.namegen._

import com.mongodb.MongoException
import com.mongodb.async.AsyncBatchCursor
import org.bson.BsonDocument
import scalaz._

package object mongodb {
  type BsonCursor         = AsyncBatchCursor[BsonDocument]
  type MongoErrT[F[_], A] = EitherT[F, MongoException, A]

  type WorkflowExecErrT[F[_], A] = EitherT[F, WorkflowExecutionError, A]

  type JavaScriptPrg           = Vector[Js.Stmt]
  type JavaScriptLogT[F[_], A] = WriterT[F, JavaScriptPrg, A]
  type JavaScriptLog[A]        = Writer[JavaScriptPrg, A]

  type MongoDbIOLog[A] = JavaScriptLogT[MongoDbIO, A]

  // TODO: parameterize over label (SD-512)
  def freshName: State[NameGen, BsonField.Name] =
    quasar.namegen.freshName("tmp").map(BsonField.Name)
}
