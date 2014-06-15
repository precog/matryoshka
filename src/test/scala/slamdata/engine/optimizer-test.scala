package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.std._

import org.specs2.mutable._

class OptimizerSpec extends Specification with CompilerHelpers {
  import StdLib._
  import structural._
  import set._
  
  import LogicalPlan._
  
  "simplify" should {
  
    "inline trivial binding" in {
      val lp = letOne('tmp0, read("foo"), free('tmp0))
    
      Optimizer.simplify(lp) should_== read("foo")
    }
  
    "not inline binding that's used twice" in {
      val lp = letOne('tmp0, 
                      read("foo"), 
                      makeObj(
                        "bar" -> ObjectProject(free('tmp0), constant(Data.Str("bar"))),
                        "baz" -> ObjectProject(free('tmp0), constant(Data.Str("baz")))
                      )
                    )
    
      Optimizer.simplify(lp) should_== lp
    }
  
    "partially inline a more interesting case" in {
      val lp = letOne('tmp0,
                  read("person"),
                    letOne('tmp1,
                      makeObj(
                        "name" -> ObjectProject(free('tmp0), constant(Data.Str("name")))
                      ),
                      letOne('tmp2,
                        OrderBy(
                          free('tmp1),
                          MakeArray(
                            ObjectProject(free('tmp1), constant(Data.Str("name")))
                          )
                        ),
                        free('tmp2)
                      )
                    )
                  )
                
      val slp = letOne('tmp1,
                  makeObj(
                    "name" -> ObjectProject(read("person"), constant(Data.Str("name")))
                  ),
                  OrderBy(
                    free('tmp1),
                    MakeArray(
                      ObjectProject(free('tmp1), constant(Data.Str("name")))
                    )
                  )
                )
            
      Optimizer.simplify(lp) should_== slp
    }

  }

}
