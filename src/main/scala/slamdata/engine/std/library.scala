package slamdata.engine.std

import slamdata.engine.Func

trait Library {
  def functions: List[Func]
}