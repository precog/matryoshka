---
layout: docs
title: A First Example
---

# {{ page.title }}

So, if you’re reading this, you _probably_ already have an AST or other recursive ADT somewhere that has gotten a bit out of hand, and you’ve heard that Matryoshka (or something about recursion schemes) may help you.

So, let’s start with a simple version of what you may already have. Here’s an AST representing a few arithmethic operations. Like most ASTs, it’s recursive – the case classes refer directly to the abstract class they’re extending.

```scala mdoc
sealed abstract class Arithmetic
final case class Number(v: Double) extends Arithmetic
final case class Add(a: Arithmetic, b: Arithmetic) extends Arithmetic
final case class Subtract(a: Arithmetic, b: Arithmetic) extends Arithmetic
final case class Multiply(a: Arithmetic, b: Arithmetic) extends Arithmetic
final case class Divide(a: Arithmetic, b: Arithmetic) extends Arithmetic
```

We gain a lot of flexibility by _separating_ that recursion from the AST. We do this with the Stephen Compall mantra of “add a type parameter!”

```scala mdoc
sealed abstract class ArithmeticF[A]
final case class NumberF[A](v: Double) extends ArithmeticF[A]
final case class AddF[A](a: A, b: A) extends ArithmeticF[A]
final case class SubtractF[A](a: A, b: A) extends ArithmeticF[A]
final case class MultiplyF[A](a: A, b: A) extends ArithmeticF[A]
final case class DivideF[A](a: A, b: A) extends ArithmeticF[A]
```

So, everywhere that referred to `Arithmetic` before now just refers to `A`, and we can fill that with whatever type we want. And, with a type parameter, come the type class instances (this should be derivable, but doesn’t yet exist in shapeless-contrib)

```scala mdoc:silent
import scalaz._, Scalaz._

implicit val traverse: Traverse[ArithmeticF] = new Traverse[ArithmeticF] {
  def traverseImpl[G[_], A, B]
    (fa: ArithmeticF[A])
    (f: A => G [B])
    (implicit G: Applicative[G]) =
    fa match {
      case NumberF(v)        => G.point(NumberF(v))
      case AddF(a, b)      => (f(a) ⊛ f(b))(AddF(_, _))
      case SubtractF(a, b) => (f(a) ⊛ f(b))(SubtractF(_, _))
      case MultiplyF(a, b) => (f(a) ⊛ f(b))(MultiplyF(_, _))
      case DivideF(a, b)   => (f(a) ⊛ f(b))(DivideF(_, _))
    }
}
```

Now is when we start wanting to take advantage of Matryoshka, so let’s bring it into scope.

```scala mdoc:silent
import matryoshka._
import matryoshka.implicits._
```

Since you already have your recursive AST, you also have code that uses it, and the last thing you want to do when trying a new tool is have to rewrite the code you already have. So, we’ll keep your AST and your code – all you have to do is define the relationship between the original AST and the new one, which we’ll do like this:

```scala mdoc:silent
val arithmeticCoalgebra: Coalgebra[ArithmeticF, Arithmetic] = {
  case Number(v)      => NumberF(v)
  case Add(a, b)      => AddF(a, b)
  case Subtract(a, b) => SubtractF(a, b)
  case Multiply(a, b) => MultiplyF(a, b)
  case Divide(a, b)   => DivideF(a, b)
}

val arithmeticAlgebra: Algebra[ArithmeticF, Arithmetic] = {
  case NumberF(v)      => Number(v)
  case AddF(a, b)      => Add(a, b)
  case SubtractF(a, b) => Subtract(a, b)
  case MultiplyF(a, b) => Multiply(a, b)
  case DivideF(a, b)   => Divide(a, b)
}

implicit val arithmeticBirecursive: Birecursive.Aux[Arithmetic, ArithmeticF] =
  Birecursive.fromAlgebraIso(arithmeticAlgebra, arithmeticCoalgebra)
```

All that this does is define the mapping between the directly-recursive structure and the functor, so it’s just a matter of seeing where the `F` suffix is.

With this relationship established, we now have access to all the power of recursion schemes.

```scala mdoc:silent
val prettyPrint: Algebra[ArithmeticF, String] = {
  case NumberF(v)      => v.toString
  case AddF(a, b)      => s"($a) + ($b)"
  case SubtractF(a, b) => s"($a) - ($b)"
  case MultiplyF(a, b) => s"($a) * ($b)"
  case DivideF(a, b)   => s"($a) / ($b)"
}
```

```scala mdoc
val expr: Arithmetic =
  Add(Multiply(Number(3), Divide(Number(4), Number(5))), Number(6))
  
Recursive[Arithmetic].cata(expr)(prettyPrint)
```

**NB**: This is just a simple example. There are much better ways to print ASTs (which also take advantage of Matryoshka), but they are too complicated for this example.

```scala mdoc:silent
val eval: Algebra[ArithmeticF, Double] = {
  case NumberF(v)      => v
  case AddF(a, b)      => a + b
  case SubtractF(a, b) => a - b
  case MultiplyF(a, b) => a * b
  case DivideF(a, b)   => a / b
}
```

```scala mdoc
Recursive[Arithmetic].cata(expr)(eval)
```

So, that’s two simple examples where we don’t have to think about recursion at all – only one level of the operation at a time. But, we can now use this to gain some efficiency:

```scala mdoc
Recursive[Arithmetic].cata(
  expr)(
  Zip[Algebra[ArithmeticF, ?]].zip(eval, prettyPrint))
```

Now we’ve generated _both_ values, but we only traversed the tree once. This is a pattern we see a lot in recursion schemes – ways to compose operations at the algebra level, such that we can do multiple operations in a single pass. And there are a bunch of “algebra transformations”, which massage these algebras into the shapes needed for different kinds of composition, so you don’t need to think about that at the point at which the algebra is defined.

A better `prettyPrint`:

```scala mdoc:silent
val precedence: ArithmeticF[_] => Int = {
  case NumberF(_)      => 0
  case AddF(_, _)      => 3
  case SubtractF(_, _) => 4
  case MultiplyF(_, _) => 1
  case DivideF(_, _)   => 2
}

def buildOp
  (currentPrecedence: Int, a: (Int, String), op: String, b: (Int, String))
    : String = {
  val newA = if (a._1 <= currentPrecedence) a._2 else s"(${a._2})"
  val newB = if (b._1 < currentPrecedence) b._2 else s"(${b._2})"

  s"$newA $op $newB"
}

val prettyPrintʹ: GAlgebra[(Int, ?), ArithmeticF, String] = {
  case NumberF(v)      => v.toString
  case AddF(a, b)      => buildOp(3, a, "+", b)
  case SubtractF(a, b) => buildOp(4, a, "-", b)
  case MultiplyF(a, b) => buildOp(1, a, "*", b)
  case DivideF(a, b)   => buildOp(2, a, "/", b)
}
```

```scala mdoc
Recursive[Arithmetic].zygo(expr)(precedence, prettyPrintʹ)
```

And now our pretty-printer only inserts necessary parens.
