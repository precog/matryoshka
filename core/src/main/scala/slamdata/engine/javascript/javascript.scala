package slamdata.engine.javascript

import scala.collection.immutable.Map

import scalaz._
import Scalaz._
import slamdata.engine.{Terminal, RenderTree}

/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013 Alexander Nemish.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
sealed trait Js {
  def block: Js.Block
  def join(a: Js): Js
  final def ++(a: Js): Js = join(a)

  def render(indent: Int): String = JavascriptPrinter.print(this, indent)
}

object Js {
  sealed trait Stmt extends Js {
    def block = Block(List(this))
    def join(a: Js) = (this, a) match {
      case (Block(lhs), Block(rhs)) => Block(lhs ::: rhs)
      case (Stmts(lhs), Block(rhs)) => Block(lhs ::: rhs)
      case (Block(lhs), Stmts(rhs)) => Block(lhs ::: rhs)
      case (Stmts(lhs), Stmts(rhs)) => Stmts(lhs ::: rhs)
      case (Block(lhs), s: Stmt) => Block(lhs :+ s)
      case (s: Stmt, Block(rhs)) => Block(s :: rhs)
      case (Stmts(lhs), s: Stmt) => Stmts(lhs :+ s)
      case (s: Stmt, Stmts(rhs)) => Stmts(s :: rhs)
      case (lhs: Stmt, rhs: Stmt) => Block(List(lhs, rhs))
    }
  }
  sealed trait Expr extends Stmt
  sealed trait Lit extends Expr

  case class Bool(value: Boolean) extends Lit
  case class Str(value: String) extends Lit
  case class Num(value: Double, isFloat: Boolean) extends Lit
  case class AnonElem(values: List[Expr]) extends Lit
  case object Unit extends Lit
  case object Null extends Lit

  case class Lazy[A <: Js](ast: () => A) extends Expr
  case class Ident(ident: String) extends Expr
  case class Raw(js: String) extends Expr
  case class Access(qualifier: Expr, key: Expr) extends Expr
  case class Select(qualifier: Expr, name: String) extends Expr
  case class UnOp(operator: String, operand: Expr) extends Expr
  case class BinOp(operator: String, lhs: Expr, rhs: Expr) extends Expr
  case class Call(callee: Expr, params: List[Expr]) extends Expr
  case class New(ctor: Expr) extends Expr
  case class Throw(expr: Expr) extends Expr
  case class AnonFunDecl(params: List[String], body: List[Stmt]) extends Expr
  case class AnonObjDecl(fields: List[(String, Expr)]) extends Expr
  case class Ternary(cond: Expr, `then`: Expr, `else`: Expr) extends Expr

  case class Block(stmts: List[Stmt]) extends Stmt
  case class Try(body: Stmt, cat: Option[Catch], fin: Option[Stmt]) extends Stmt
  case class Catch(ident: Ident, body: Stmt) extends Stmt
  case class If(cond: Expr, `then`: Stmt, `else`: Option[Stmt]) extends Stmt
  case class While(cond: Expr, body: Stmt) extends Stmt
  case class For(init: List[Stmt], check: Expr, update: List[Stmt], body: Stmt) extends Stmt
  case class ForIn(ident: Ident, coll: Expr, body: Stmt) extends Stmt
  sealed trait Switchable extends Stmt
  case class Case(const: List[Expr], body: Stmt) extends Switchable
  case class Default(body: Stmt) extends Switchable
  case class Switch(expr: Expr, cases: List[Case], default: Option[Default]) extends Stmt
  case class VarDef(idents: List[(String, Expr)]) extends Stmt
  case class FunDecl(ident: String, params: List[String], body: List[Stmt]) extends Stmt
  case class ObjDecl(name: String, constructor: FunDecl, fields: List[(String, Expr)]) extends Stmt
  case class Return(jsExpr: Expr) extends Stmt
  case class Stmts(stmts: List[Stmt]) extends Stmt

  // Because they're not just identifiers.
  val This = Ident("this")
  val Undefined = Ident("undefined")

  /** Pattern matching valid identifiers. */
  val SimpleNamePattern = "[_a-zA-Z][_a-zA-Z0-9]*".r

  // Some functional-style helpers
  def Let(bindings: Map[String, Expr], stmts: List[Stmt], expr: Expr) = {
    val (params, args) = bindings.toList.unzip
    Call(AnonFunDecl(params, stmts :+ Return(expr)), args)
  }
  def BlockExpr(thisArg: Option[Expr], stmts: List[Stmt], expr: Expr) =
    thisArg match {
      case None => Call(AnonFunDecl(Nil, stmts :+ Return(expr)), Nil)
      case Some(arg) =>
        Call(
          Select(AnonFunDecl(Nil, stmts :+ Return(expr)), "call"),
          List(arg))
    }

  // TODO: Kill this once all its call sites have been changed to JsCore.
  def safeCall(obj: Expr, fn: String, args: List[Expr]): Expr =
    obj match {
      case Null   => Null
      case _: Lit => Call(Select(obj, fn), args)
      case _      =>
        Ternary(BinOp("!=", obj, Null), Call(Select(obj, fn), args), Null)
    }

  implicit val JSRenderTree = new RenderTree[Js] {
    override def render(v: Js) = Terminal(v.render(2), "JavaScript" :: Nil)
  }
}

private object JavascriptPrinter {
  import Js._

  private[this] val substitutions = Map("\\\"".r -> "\\\\\"", "\\n".r -> "\\\\n", "\\r".r -> "\\\\r", "\\t".r -> "\\\\t")
  private[this] def simplify(ast: Js): Js = ast match {
    case Block(stmts) => Block(stmts.filter(_ != Unit))
    case Case(const, Block(List(stmt))) => Case(const, stmt)
        case Default(Block(List(stmt))) => Default(stmt)
    case t => t
  }

  def print(ast: Js, indent: Int): String = {
    def ind(c: Int = 0) = " " * (indent + c)
    def p(ast: Js) = print(ast, indent)
    def p2(ast: Js) = ind(2) + p3(ast)
    def p3(ast: Js) = print(ast, indent + 2)
    def p4(ast: Js) = ind() + p(ast)
    def s(ast: Js) = ast match {
      case _: Lit => p(ast)
      case _: Ident => p(ast)
      case _: Call => p(ast)
      case _: Select => p(ast)
      case _: Access => p(ast)
      case s => s"(${p(ast)})"
    }
    def seqOut(content: List[String], open: String, sep: String, close: String) =
      if (content.foldLeft(indent + open.length + close.length)(_ + _.length + sep.length + 1) < 80 && !content.exists(_.contains('\n')))
        content.mkString(open, sep + " ", close)
      else
        content.mkString(open + "\n" + ind(2), sep + "\n" + ind(2), close)
    def blockOut(content: List[String], open: String, sep: String, close: String) =
      if (content.foldLeft(indent + open.length + close.length)(_ + _.length + sep.length + 1) < 80 && !content.exists(_.contains('\n')))
        content.mkString(open + " ", sep + " ", " " + close)
      else
        content.mkString(open + "\n" + ind(2), sep + "\n" + ind(2), "\n" + ind() + close)

    simplify(ast) match {
      case Lazy(f)                            => p(f())
      case Null                               => "null"
      case Bool(value)                        => value.toString
      case Str(value)                         => "\"" + substitutions.foldLeft(value){case (v, (r, s)) => r.replaceAllIn(v, s)} + "\""
      case Num(value, true)                   => value.toString
      case Num(value, false)                  => value.toLong.toString
      case AnonElem(values)                   =>
        seqOut(values.map(p3), "[", ",", "]")
      case Ident(value)                       => value
      case Raw(value)                         => value
      case Access(qual, key)                  => s"${s(qual)}[${p(key)}]"
      case Select(qual, name)                 => s"${s(qual)}.$name"
      case UnOp(operator, operand)            => operator + " " + s(operand)
      case BinOp("=", lhs, rhs)               => s"${p(lhs)} = ${p(rhs)}"
      case BinOp(operator, lhs @ BinOp(_, _, _), rhs @ BinOp(_, _, _)) =>
        s"${s(lhs)} $operator ${s(rhs)}"
      case BinOp(operator, lhs @ BinOp(_, _, _), rhs) => s"${s(lhs)} $operator ${p(rhs)}"
      case BinOp(operator, lhs, rhs)          => s"${s(lhs)} $operator ${s(rhs)}"
      case New(call)                          => s"new ${p(call)}"
      case Throw(expr)                        => s"throw ${p(expr)}"
      case Call(callee @ Ident(_), params)    =>
        seqOut(params.map(p3), p(callee) + "(", ",", ")")
      case Call(callee @ Select(_, _), params) =>
        seqOut(params.map(p3), p(callee) + "(", ",", ")")
      case Call(callee, params)               =>
        seqOut(params.map(p3), s"""(${p(callee)})(""", ",", ")")
      case Block(Nil)                         => "{}"
      case Block(stmts)                       =>
        blockOut(stmts.map(p3), "{", ";", "}")
      case Ternary(cond, thenp, elsep)        => s"${s(cond)} ? ${p(thenp)} : ${p(elsep)}"
      case If(cond, thenp, elsep)             => s"if (${p(cond)}) ${p(thenp)}" + elsep.map(e => s" else ${p(e)}").getOrElse("")
      case Switch(expr, cases, default)       =>
        blockOut(default.foldLeft(cases.map(p2))((l, d) => l :+ p2(d)), s"switch (${p(expr)}) {", "", "}")
      case Case(consts, body)                 => consts.map(c => s"case ${p(c)}:\n").mkString(ind()) + p2(body) + ";\n" + ind(2) + "break;"
      case Default(body)                      => "default:\n" + p2(body) + ";\n" + ind(2) + "break;"
      case While(cond, body)                  => s"while (${p(cond)}) ${p(body)}"
      case Try(body, cat, fin)                =>
        val b = p(body)
        val c = cat.map(p2).getOrElse("")
        val f = fin.map(f => s"finally {${p2(f)}\n}").getOrElse("")
        s"try { $b \n} $c \n $f"
      case Catch(Ident(ident), body)          => s"catch($ident) {\n${p2(body)}\n}"
      case For(init, check, update, body)     =>
        val in = init.map(p).mkString(", ")
        val upd = update.map(p).mkString(", ")
        s"for ($in; ${p(check)}; $upd) ${p(body)}"
      case ForIn(Ident(ident), coll, body)    => s"for (var $ident in (${p(coll)})) ${p(body)}"
      case VarDef(Nil)                        => sys.error("Var definition must have at least one identifier.")
      case VarDef(idents)                     =>
        "var " + idents.map {
          case (ident, Unit) => ident
        case (ident, init) => ident + " = " + p(init)
      }.mkString(", ")
      case FunDecl(ident, params, body)       => s"""function $ident(${params.mkString(", ")}) ${p(Block(body))}"""
      case AnonFunDecl(params, body)          => s"""function (${params.mkString(", ")}) ${p(Block(body))}"""
      case AnonObjDecl(fields)                =>
        if (fields.isEmpty) "{}"
        else
          blockOut(fields.map { case (k, v) => s""""$k": ${p3(v)}""" }, "{", ",", "}")
      case ObjDecl(name, FunDecl(_, params, stmts), fields) =>
        val fs = for ((n, v) <- fields) yield ind(2) + s"this.$n = ${p(v)};"
        val body = fs ++ stmts.map(s => ind(2) + p(s)) mkString "\n"
        s"""function $name(${params.mkString(", ")}) {\n$body\n${ind()}}"""
      case Return(jsExpr)                     => s"return ${p(jsExpr)}"
      case Unit                               => ""
      case Stmts(stmts)                       => stmts.map(p).mkString("", ";\n", ";\n")
    }
  }
}
