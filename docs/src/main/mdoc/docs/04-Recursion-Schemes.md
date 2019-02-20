---
layout: docs
title: Recursion Schemes
---

```scala mdoc
import matryoshka._, implicits._, data._
import scalaz._, Scalaz._
```
    
# {{ page.title }}

We already came accross some recursion schemes like `cata` or `zygo` but without much explanation about them so far. As the name suggests, recursion schemes abstract away the different kinds of recursive traversals, enabling us to focus on *what* we do with each element of a recursive structure rather than on *how* we gain access to each element. 

One can divide recursion schemes in three kinds:

* **folds** that recursively apply an `Algebra` (something of the shape `F[A] => A`) on a structure of nested `F`s in order to "collapse" it to an `A`.
* **unfolds** that recursively apply a `Coalgebra`(of shape `A => F[A])` on a `A` to "expand" it to bigger and bigger nestings of `F[A]`s. 
* **refolds** that combine a unfold and a fold in a single traversal.

There are various recursion schemes of each of these three kinds. Within a same kind, all schemes share the same way of walking a structure, but differ by what they carry along that traversal and hence by the shape of the (co)algebra they apply at each step.

Lets build an intuition of how each kind of scheme walks through a given recursive structure, using both an `Algebra` and a `Coalgebra`. 

We'll use a very simple binary tree with `Int` labelling each node and leaf.

```scala mdoc:silent
sealed trait Tree[A]
final case class Node[A](label: Int, left: A, right: A) extends Tree[A]
final case class Leaf[A](label: Int)                    extends Tree[A]
final case class EmptyTree[A]()                         extends Tree[A]

```

To use that as our recursive structure, we'll need to provide at least a `Functor` instance for it (we'll see why this is important soon). Some schemes have greater needs and require a more powerful `Traverse` instance.

```scala mdoc:silent
implicit val treeFunctor: Functor[Tree] = new Functor[Tree] {
  def map[A, B](fa: Tree[A])(f: A => B): Tree[B] = fa match {
    case Node(label, l, r) => Node(label, f(l), f(r))
    case Leaf(label)       => Leaf[B](label)
    case EmptyTree()       => EmptyTree[B]()
  }
}
```

Now we can (almost) easily create a simple binary search tree:

```scala mdoc:silent
//     42
//    /  \
//   28  83
//  /  \
// 12  32
//    /  \
//   29  35
val tree: Fix[Tree] = Fix(Node(
             42,
             Fix(Node(
               28,
               Fix(Leaf(12)),
               Fix(Node(
                 32,
                 Fix(Leaf(29)),
                 Fix(Leaf(35))
               ))
             )),
             Fix(Leaf(83))
           ))
```


## Folds

The simplest possible fold is `cata`. It applies an `Algebra[F, A]` "bottom-up" to a recursive structure `F`. It first gets down to the deepest `F`s in our structure, applies the algebra to obtain an `A`, plugs it back into the `F` one level up (it uses `F`'s `map` to do so, that's why we need `F` to have a `Functor` instance), and continues recursively until it reaches the top of our initial structure, returning the resulting `A`. 

Let's use a (very) dirty trick to show in which order `cata` traverses the nodes of our above `tree`.

```scala mdoc
// This is very bad!
// Kids, don't use side-effects like this at home
val printLabels: Algebra[Tree, Unit] = {
  case Node(l, _, _) => println(s"visiting node $l")
  case Leaf(l)       => println(s"visiting leaf $l")
  case EmptyTree()   => println(s"visiting empty")
}

tree.cata(printLabels)
```

Although really awful (seriously there are better ways to do that, we'll show that in a moment), this showed us a very important point: folds like `cata` traverse a recursive structure in such a way that the provided `Algebra` is called in turn on each leftmost, deepest, not-yet-seen element until it reaches the root of the structure.

But we are yet to understand what makes it interesting to use this convoluted `Tree[A]` rather than the simpler, directly recursive `Tree` we would have usually writen.

Suppose now that we want to to extract the ordered list of all labels in `tree`, provided it represents a binary search tree (a tree such that for every node labelled wit `x`, all nodes under its left branch have a label `y < x` and all nodes under its right branch have a label `z >= x`).

Using a classic recursive approach, starting from a `Node`, we would recursively build the list obtained from its `left` and `right` branches and concatenate those lists with its `label` in the middle.

But recursion schemes are all about abstracting recursive traversals, right? So using `cata`, we only need to define **what** we want to do with each node of our tree, we don't need to specify **how** we traverse the whole tree.

```scala mdoc:silent
val merge: Algebra[Tree, List[Int]] = {
  case Node(i, l, r) => l ++ List(i) ++ r 
  case Leaf(i)       => List(i)
  case EmptyTree()   => Nil
}
```
Using this algebra with a `cata` on our `tree` should yield the ordered list of the labels of each node/leaf. 
```scala mdoc
tree.cata(merge)
```
Tadaaaa! 

The main takeway here is that in our algebra, we manipulate nodes where the sub-nodes (the `A`s of our `Tree[A]`) have been replaced by the result of (recursively) applying the algebra to them. 

## Unfolds

Unfolds are the dual of folds. They use a `Coalgebra[F, A]` (again, something of the shape `A => F[A]`) to produce a recursive structure out of an `A`. The simplest possible unfold is called `ana`.

By duality, it's easy to see that unfolds work "top-down". In other words, an unfold producing a `Tree` will first produce its root, then the root's children, and so on.

For example, let's use an unfold to produce a binary search tree from a list of integers. Given a list, we partition its tail depending on whereas its elements are smaller or greater than the head of the original list. Using that we can build a `Node(head, smaller, greater)` and let `ana` recursively build trees for the `smaller` and `greater` list.

```scala mdoc:silent
val split: Coalgebra[Tree, List[Int]] = { 
  case Nil          => EmptyTree()
  case head :: Nil  => Leaf(head)
  case head :: tail => 
    val (smaller, greater) = tail.partition(_ < head)
    Node(head, smaller, greater)
}
  
```
If you squint hard enough, you might convince yourself that this indeed produces a binary search tree for our input list.
```scala mdoc
List(42, 45, 28, 32, 12, 48).ana[Fix[Tree]](split)
```

## Refolds

As we said earlier, refolds combine an unfold and a fold in a single traversal. For example, the simplest refold is `hylo`, it combines an `ana` and a `cata`.

This means that `hylo` uses a `Coalgebra[F, A]` and an `Algebra[F, B]` to transform `A`s into `B`s. But it does so in a way that is much more efficient than simply applying an `ana` and then a `cata`. In fact, `hylo` builds just as much of the recursive structure (using the coalgebra) as it is needed to reach a point where it can start to collapse it using the algebra.

In other words, in our tree example, `hylo` would only build a branch of the tree and start to collapse that branch as soon as it reaches a leaf, therefore never build the whole tree.

We can use the coalgebra and algebra we defined above to transform a `List[Int]` into a `List[Int]`, this would (partially) build a binary search tree from the input list and traverse the elements of that tree in ascending order, effecively sorting the whole list.

```scala mdoc
List(42, 45, 28, 32, 42, 12, 48, 1, 64, 8, 0).hylo(merge, split)
```

Looks like we've just implemented a quicksort!
