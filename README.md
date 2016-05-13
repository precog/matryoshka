# Matryoshka

Generalized folds, unfolds, and traversals for fixed point data structures in Scala.

[![Typelevel incubator](https://img.shields.io/badge/typelevel-incubator-F51C2B.svg)](http://typelevel.org)
[![Build Status](https://travis-ci.org/slamdata/matryoshka.svg?branch=master)](https://travis-ci.org/slamdata/matryoshka)
[![codecov.io](https://codecov.io/github/slamdata/matryoshka/coverage.svg?branch=master)](https://codecov.io/github/slamdata/matryoshka?branch=master)
[![Join the chat at https://gitter.im/slamdata/matryoshka](https://badges.gitter.im/slamdata/matryoshka.svg)](https://gitter.im/slamdata/matryoshka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## External Resources

- [Functional Programming with Bananas, Lenses, Envelopes and Barbed Wire](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.41.125) – the iconic paper that collected a lot of this info for the first time
- [Recursion Schemes: A Field Guide (Redux)](http://comonad.com/reader/2009/recursion-schemes/) – Ed Kmett’s summary of various folds and unfolds, with links to Haskell code
- [Unifying Structured Recursion Schemes](http://www.cs.ox.ac.uk/people/jeremy.gibbons/publications/urs.pdf) – a newer paper on how to generalize recursion schemes
- [Efficient Nanopass Compilers using Cats and Matryoshka](https://github.com/sellout/recursion-scheme-talk/blob/master/nanopass-compiler-talk.org) – Greg Pfeil’s talk on this library (and some other things)
- [Fix Haskell (by eliminating recursion)](https://github.com/sellout/recursion-scheme-talk/blob/master/recursion-scheme-talk.org) – Greg Pfeil’s talk on recursion schemes in Haskell

## Usage

To use Matryoshka, the following import should bring in pretty much everything:

```scala
import matryoshka._, Recursive.ops._, TraverseT.ops._
```

Also, there are a number of implicits that scalac has trouble finding. Adding @milessabin’s [SI-2712 fix compiler plugin](https://github.com/milessabin/si2712fix-plugin) will simplify a ton of Matryoshka-related code.

## Introduction

This library is predicated on the idea of rewriting your recursive data structures, replacing the recursive type reference with a fresh type parameter.

```scala
sealed trait Expr
final case class Num(value: Long)      extends Expr
final case class Mul(l: Expr, r: Expr) extends Expr
```

could be rewritten as

```scala
sealed trait Expr[A]
final case class Num[A](value: Long) extends Expr[A]
final case class Mul[A](l: A, r: A)  extends Expr[A]
```

This trait generally allows a `Traverse` instance (or at least a `Functor` instance). Then you use one of the fixed point type constructors below to regain your recursive type.

### Fixpoint Types

These types take a one-arg type constructor and provide a recursive form of it.

All of these types have instances for `Recursive`, `Corecursive`, `FunctorT`, `TraverseT`, `Equal`, `Show`, and `Arbitrary` type classes unless otherwise noted.

- `Fix` – This is the simplest fixpoint type, implemented with general recursion.
- `Mu` – This is for inductive (finite) recursive structures, models the concept of “data”, aka, the “least fixed point”.
- `Nu` – This is for coinductive (potentially infinite) recursive structures, models the concept of “codata”, aka, the “greatest fixed point”.
- `Cofree[?[_], A]` – Only has a `Corecursive` instance if there’s a `Monoid` for `A`. This represents a structure with some metadata attached to each node. In addition to the usual operations, it can also be folded using an Elgot algebra.
- `Free[?[_], A]` – Does not have a `Recursive` instance. In addition to the usual operations, it can also be created by unfolding with an Elgot coalgebra.

So a type like `Mu[Expr]` is now isomorphic to the original recursive type. However, the point is to avoid operating on recursive types directly …

### Algebras

A structure like this makes it possible to separate recursion from your operations. You can now write transformations that operate on only a single node of your structure at a time.

![algebras and coalgebras](resources/algebras.png)

This diagram covers the major classes of transformations. The most basic ones are in the center and the arrows show how they can be generalized in various ways.

Here is a very simple example of an algebra (`eval`) and how to apply it to a recursive structure.

```scala
val eval: Expr[Long] => Long = {
  case Num(x)    => x
  case Mul(x, y) => x * y
}

def someExpr[T[_[_]]: Corecursive]: T[Expr] =
  Mul(Num[T[Expr]](2).embed, Mul(Num[T[Expr]](3).embed,
      Num[T[Expr]](4).embed).embed).embed

someExpr[Mu].cata(eval) // ⇒ 24
```

The `.embed` calls in `someExpr` wrap the nodes in the fixed point type. `embed` is generic, and we abstract `someExpr` over the fixed point type (only requiring that it has an instance of `Corecursive`), so we can postpone the choice of the fixed point as long as possible.
 
### Folds

Those algebras can be applied recursively to your structures using many different folds. `cata` in the example above is the simplest fold. It traverses the structure bottom-up, applying the algebra to each node. That is the general behavior of a fold, but more complex ones allow for various comonads and monads to affect the result.

Here is a cheat-sheet (also available [in PDF](resources/recursion-schemes.pdf)) for some of them.

![folds and unfolds](resources/recursion-schemes.png)

## Contributing

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

## Users

- [Quasar](https://github.com/quasar-analytics/quasar)
