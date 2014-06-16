package slamdata.java

object JavaUtil {
  def stackTrace(t: java.lang.Throwable): String = {
    import java.io._
    
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    t.printStackTrace(pw)
    pw.flush
    
    sw.toString
  }
}