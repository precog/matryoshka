---
layout: docs
title: Algebras
---

```tut:book
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._
```

# {{ page.title }}

or, “I know what algebra is, and these ain’t it.”

The algebras (and coalgebras, etc.) are more properly known as “F-algebras”. To connect these back to the algebra we’re all generally familiar with from school, let’s look at a simple ADT –

```tut:silent
sealed trait Expr[A]
final case class Mul[A](a: A, b: A)  extends Expr[A]
final case class Add[A](a: A, b: A)  extends Expr[A]
final case class Num[A](i: Int)      extends Expr[A]

implicit val exprTraverse: Traverse[Expr] = new Traverse[Expr] {
  def traverseImpl[G[_], A, B]
    (fa: Expr[A])
    (f: A => G[B])
    (implicit G: Applicative[G]) =
    fa match {
      case Mul(a, b) => (f(a) ⊛ f(b))(Mul(_, _))
      case Add(a, b) => (f(a) ⊛ f(b))(Add(_, _))
      case Num(v) => G.point(Num(v))
  }
}
```

We can write out a simple expression

```tut:book
val expr = Add(Mul(Add(Num[Mu[Expr]](2).embed, Num[Mu[Expr]](3).embed).embed, Num[Mu[Expr]](4).embed).embed,
               Add(Mul(Num[Mu[Expr]](5).embed, Num[Mu[Expr]](6).embed).embed, Num[Mu[Expr]](7).embed).embed).embed
```
which would have looked like **(2 + 3) * 4 + 5 * 6 + 7** back in high school. To make the precedence a bit more explicit, here are some extra parens: **((2 + 3) * 4) + ((5 * 6) + 7)**.

Now, how do we solve / evaluate this? If you’re anything like me, you take a few steps:

1. **((2 + 3) * 4) + ((5 * 6) + 7)**
2. **(   5     * 4) + (  30   + 7)**
3. **      20     +     37      **
4. **            57            **

So, there are two aspects to this. First, there are some simple rules:

```tut:book
val eval: Algebra[Expr, Int] = {
  // 1. + means to add two numbers together
  case Add(x, y) => x + y
  // 2. * means to multiply two numbers together
  case Mul(x, y) => x * y
  // 3. a number simply represents itself
  case Num(x)    => x
}
```
Hey, look at that – this evaluation rule is an “algebra”. And it’s just a simplified version of the particular algebra we grew up with.

The second aspect involves applying these rules from most deeply nested expressions out.

```tut:book
expr.cata(eval)
```
There it is. You’ve got an algebra and applied it.

## Whence from here?

Well, you’re already familiar with algebraic data types in general (which get their name via a different path from algebra).
