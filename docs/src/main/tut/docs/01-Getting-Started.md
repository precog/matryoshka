---
layout: docs
title: Getting Started
---

## {{ page.title }}

```scala
libraryDependencies += "com.slamdata" %% "matryoshka-core" % "0.22.0"
```

```tut:silent
import matryoshka._
import matryoshka.implicits._
```

To use a specific fixed-point type (like `Mu`, `Nu`, or `Fix`), you will also need

```tut:silent
import matryoshka.data._
```

However, we generally recommend that you abstract over the fixed-point type, and use the `Recursive` type constraint.

Then hang out on [Gitter](https://gitter.im/slamdata/matryoshka).
