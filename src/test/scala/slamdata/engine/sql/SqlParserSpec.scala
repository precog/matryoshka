package slamdata.engine.sql

import org.specs2.mutable._

class SQLParserSpec extends Specification {
  import SqlQueries._

  implicit def stringToQuery(s: String): Query = Query(s)
  
  "SQLParser" should {
    "parse query1" in {
      val parser = new SQLParser
      val r = parser.parse(q1).toOption
      r should beSome
    }

    "parse query2" in {
      val parser = new SQLParser
      val r = parser.parse(q2).toOption
      r should beSome
    }

    "parse query3" in {
      val parser = new SQLParser
      val r = parser.parse(q3).toOption 
      r should beSome
    }

    "parse query4" in {
      val parser = new SQLParser
      val r = parser.parse(q4).toOption
      r should beSome
    }

    "parse query5" in {
      val parser = new SQLParser
      val r = parser.parse(q5).toOption
      r should beSome
    }

    "parse query6" in {
      val parser = new SQLParser
      val r = parser.parse(q6).toOption
      r should beSome
    }

    "parse query7" in {
      val parser = new SQLParser
      val r = parser.parse(q7).toOption
      r should beSome
    }

    "parse query8" in {
      val parser = new SQLParser
      val r = parser.parse(q8).toOption
      r should beSome
    }

    "parse query9" in {
      val parser = new SQLParser
      val r = parser.parse(q9).toOption
      r should beSome
    }

    "parse query10" in {
      val parser = new SQLParser
      val r = parser.parse(q10).toOption
      r should beSome
    }

    "parse query11" in {
      val parser = new SQLParser
      val r = parser.parse(q11).toOption
      r should beSome
    }

    "parse query12" in {
      val parser = new SQLParser
      val r = parser.parse(q12).toOption
      r should beSome
    }

    "parse query13" in {
      val parser = new SQLParser
      val r = parser.parse(q13).toOption
      r should beSome
    }

    "parse query14" in {
      val parser = new SQLParser
      val r = parser.parse(q14).toOption
      r should beSome
    }

    "parse query16" in {
      val parser = new SQLParser
      val r = parser.parse(q16).toOption
      r should beSome
    }

    "parse query17" in {
      val parser = new SQLParser
      val r = parser.parse(q17).toOption
      r should beSome
    }

    "parse query18" in {
      val parser = new SQLParser
      val r = parser.parse(q18).toOption
      r should beSome
    }

    "parse query19" in {
      val parser = new SQLParser
      val r = parser.parse(q19).toOption
      r should beSome
    }

    "parse query20" in {
      val parser = new SQLParser
      val r = parser.parse(q20).toOption
      r should beSome
    }

    "parse query21" in {
      val parser = new SQLParser
      val r = parser.parse(q21).toOption
      r should beSome
    }

    "parse query22" in {
      val parser = new SQLParser
      val r = parser.parse(q22).toOption
      r should beSome
    }
  }
}
