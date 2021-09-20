package org.apache.spark.secco.trees

import org.apache.commons.lang3.ClassUtils
import org.apache.spark.secco.execution.SharedParameter
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.util.Utils

import scala.collection.Map
import scala.reflect.ClassTag

// scalastyle:off
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
  // scalastyle:on
  self: BaseType =>

  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  private lazy val _hashCode: Int =
    scala.util.hashing.MurmurHash3.productHash(this)
  override def hashCode(): Int = _hashCode

  /**
    * Faster version of equality which short-circuits when two treeNodes are the same instance.
    * We don't just override Object.equals, as doing so prevents the scala compiler from
    * generating case class `equals` methods
    */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
    * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
    * The condition is recursively applied to this node and all of its children (pre-order).
    */
  def find(f: BaseType => Boolean): Option[BaseType] =
    if (f(this)) {
      Some(this)
    } else {
      children.foldLeft(Option.empty[BaseType]) { (l, r) =>
        l.orElse(r.find(f))
      }
    }

  /**
    * Runs the given function on this node and then recursively on [[children]].
    * @param f the function to be applied to each node in the tree.
    */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
    * Runs the given function recursively on [[children]] then on this node.
    * @param f the function to be applied to each node in the tree.
    */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
    * Returns a Seq containing the result of applying the given function to each
    * node in this tree in a preorder traversal.
    * @param f the function to be applied.
    */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  /**
    * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
    * resulting collections.
    */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret
  }

  /**
    * Returns a Seq containing the result of applying a partial function to all elements in this
    * tree on which the function is defined.
    */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret
  }

  /**
    * Returns a Seq containing the leaves in this tree.
    */
  def collectLeaves(): Seq[BaseType] = {
    this.collect { case p if p.children.isEmpty => p }
  }

  /**
    * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
    * is defined (pre-order), and applies the partial function to it.
    */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(Option.empty[B]) { (l, r) =>
        l.orElse(r.collectFirst(pf))
      }
    }
  }

  /**
    * Efficient alternative to `productIterator.map(f).toArray`.
    */
  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

  /**
    * Returns a copy of this node with the children replaced.
    * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
    */
  def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    def mapTreeNode(node: TreeNode[_]): TreeNode[_] = {
      val newChild = remainingNewChildren.remove(0)
      val oldChild = remainingOldChildren.remove(0)
      if (newChild fastEquals oldChild) {
        oldChild
      } else {
        changed = true
        newChild
      }
    }
    def mapChild(child: Any): Any =
      child match {
        case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
        case nonChild: AnyRef                       => nonChild
        case null                                   => null
      }
    val newArgs = mapProductIterator {
      // Handle Seq[TreeNode] in TreeNode parameters.
      case s: Stream[_] =>
        // Stream is lazy so we need to force materialization
        s.map(mapChild).force
      case s: Seq[_] =>
        s.map(mapChild)
      case m: Map[_, _] =>
        // `mapValues` is lazy and we need to force it to materialize
        m.mapValues(mapChild).view.force
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      case nonChild: AnyRef                       => nonChild
      case null                                   => null
    }

    if (changed) makeCopy(newArgs) else this
  }

  /**
    * Returns a copy of this node where `rule` has been recursively applied to the tree.
    * When `rule` does not apply to a given node it is left unchanged.
    * Users should not expect a specific directionality. If a specific directionality is needed,
    * transformDown or transformUp should be used.
    *
    * @param rule the function use to transform this nodes children
    */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
    * Returns a copy of this node where `rule` has been recursively applied to it and all of its
    * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
    *
    * @param rule the function used to transform this nodes children
    */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = rule.applyOrElse(this, identity[BaseType])

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      mapChildren(_.transformDown(rule))
    } else {
      afterRule.mapChildren(_.transformDown(rule))
    }
  }

  /**
    * Returns a copy of this node where `rule` has been recursively applied first to all of its
    * children and then itself (post-order). When `rule` does not apply to a given node, it is left
    * unchanged.
    *
    * @param rule the function use to transform this nodes children
    */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = mapChildren(_.transformUp(rule))
    if (this fastEquals afterRuleOnChildren) {
      rule.applyOrElse(this, identity[BaseType])
    } else {
      rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
    }
  }

  /**
    * Returns a copy of this node where `f` has been applied to all the nodes children.
    */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    if (children.nonEmpty) {
      var changed = false
      def mapChild(child: Any): Any =
        child match {
          case arg: TreeNode[_] if containsChild(arg) =>
            val newChild = f(arg.asInstanceOf[BaseType])
            if (!(newChild fastEquals arg)) {
              changed = true
              newChild
            } else {
              arg
            }
          case tuple @ (arg1: TreeNode[_], arg2: TreeNode[_]) =>
            val newChild1 = if (containsChild(arg1)) {
              f(arg1.asInstanceOf[BaseType])
            } else {
              arg1.asInstanceOf[BaseType]
            }

            val newChild2 = if (containsChild(arg2)) {
              f(arg2.asInstanceOf[BaseType])
            } else {
              arg2.asInstanceOf[BaseType]
            }

            if (!(newChild1 fastEquals arg1) || !(newChild2 fastEquals arg2)) {
              changed = true
              (newChild1, newChild2)
            } else {
              tuple
            }
          case other => other
        }

      val newArgs = mapProductIterator {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = f(arg.asInstanceOf[BaseType])
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case Some(arg: TreeNode[_]) if containsChild(arg) =>
          val newChild = f(arg.asInstanceOf[BaseType])
          if (!(newChild fastEquals arg)) {
            changed = true
            Some(newChild)
          } else {
            Some(arg)
          }
        case s: SharedParameter[_] => s
        case m: Map[_, _] =>
          m.mapValues {
            case arg: TreeNode[_] if containsChild(arg) =>
              val newChild = f(arg.asInstanceOf[BaseType])
              if (!(newChild fastEquals arg)) {
                changed = true
                newChild
              } else {
                arg
              }
            case other => other
          }.view
            .force // `mapValues` is lazy and we need to force it to materialize
        case args: Stream[_] =>
          args.map(mapChild).force // Force materialization on stream
        case args: Traversable[_] => args.map(mapChild)
        case nonChild: AnyRef     => nonChild
        case null                 => null
      }
      if (changed) makeCopy(newArgs) else this
    } else {
      this
    }
  }

  /**
    * Args to the constructor that should be copied, but not transformed.
    * These are appended to the transformed args automatically by makeCopy
    * @return
    */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  /**
    * Creates a copy of this type of tree node after a transformation.
    * Must be overridden by child classes that have constructor arguments
    * that are not present in the productIterator.
    * @param newArgs the new product arguments.
    */
  def makeCopy(newArgs: Array[AnyRef]): BaseType =
    attachTree(this, "makeCopy") {
      // Skip no-arg constructors that are just there for kryo.
      val ctors = getClass.getConstructors.filter(_.getParameterTypes.size != 0)
      if (ctors.isEmpty) {
        sys.error(s"No valid constructor for $nodeName")
      }
      val allArgs: Array[AnyRef] = if (otherCopyArgs.isEmpty) {
        newArgs
      } else {
        newArgs ++ otherCopyArgs
      }
      val defaultCtor = ctors
        .find { ctor =>
          if (ctor.getParameterTypes.length != allArgs.length) {
            false
          } else if (allArgs.contains(null)) {
            // if there is a `null`, we can't figure out the class, therefore we should just fallback
            // to older heuristic
            false
          } else {
            val argsArray: Array[Class[_]] = allArgs.map(_.getClass)
            ClassUtils.isAssignable(
              argsArray,
              ctor.getParameterTypes,
              true /* autoboxing */
            )
          }
        }
        .getOrElse(
          ctors.maxBy(_.getParameterTypes.length)
        ) // fall back to older heuristic

      try {
        defaultCtor.newInstance(allArgs.toArray: _*).asInstanceOf[BaseType]
      } catch {
        case e: java.lang.IllegalArgumentException =>
          throw new TreeNodeException(this, s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |types: ${newArgs.map(_.getClass).mkString(", ")}
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
      }
    }

  def nodeName: String = getClass.getSimpleName.replaceAll("Exec$", "")

//  protected def stringArgs: Iterator[Any] = productIterator

  def argString: String =
    productIterator
      .flatMap {
        case tn: TreeNode[_] if children.contains(tn)       => Nil
        case Some(tn: TreeNode[_]) if children.contains(tn) => Nil
        case Some(tn: TreeNode[_])                          => tn.simpleString :: Nil
//        case lop: LocalExec                                 => lop :: Nil
        case tn: TreeNode[_] => tn.simpleString :: Nil
        case seq: Seq[Any] if seq.toSet.subsetOf(children.toSet) =>
          Nil
        case sharedParameterSeq: Array[_]
            if sharedParameterSeq.forall(_.isInstanceOf[SharedParameter[_]]) =>
          Nil
        case iter: Iterable[_] if iter.isEmpty => Nil
        case lSeq: Seq[_] if lSeq.forall(_.isInstanceOf[LogicalPlan]) =>
          Utils.truncatedString(
            lSeq.map(_.asInstanceOf[LogicalPlan].nodeName),
            "[",
            ", ",
            "]"
          ) :: Nil
        case seq: Seq[_] => Utils.truncatedString(seq, "[", ", ", "]") :: Nil
        case set: Set[_] =>
          Utils.truncatedString(set.toSeq, "{", ", ", "}") :: Nil
        case array: Array[_] if array.isEmpty => Nil
        case array: Array[_] =>
          Utils.truncatedString(array, "[", ", ", "]") :: Nil
        case sharedParameter: SharedParameter[_] =>
          val res = sharedParameter.res
          res match {
            case iter: Iterable[_] if iter.isEmpty => Nil
            case seq: Seq[_] =>
              Utils.truncatedString(seq, "[", ", ", "]") :: Nil
            case m: Map[_, _] =>
              Utils.truncatedString(
                m.toList.map(f => s"${f._1} -> ${f._2}"),
                "{",
                ", ",
                "}"
              ) :: Nil
            case _ => Nil
          }
        case null       => Nil
        case None       => Nil
        case Some(null) => Nil
        case Some(any)  => any :: Nil
        case other      => other :: Nil
      }
      .mkString("[", ", ", "]")

  def simpleString: String = s"$nodeName$argString".trim

  def verboseString: String

  def verboseStringWithSuffix: String = verboseString

  override def toString: String = treeString

  def treeString: String = treeString(verbose = true)

  def treeString(verbose: Boolean, addSuffix: Boolean = false): String = {
    generateTreeString(
      0,
      Nil,
      new StringBuilder,
      verbose = verbose,
      addSuffix = addSuffix
    ).toString
  }

  def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false
  ): StringBuilder = {

    if (depth > 0) {
      var count = 0
      lastChildren.init.foreach { isLast =>
        builder.append(if (isLast) "   " else ":  ")
      }
      builder.append(if (lastChildren.last) "+- " else ":- ")
    }

    val str = if (verbose) {
      if (addSuffix) verboseStringWithSuffix else verboseString
    } else {
      simpleString
    }

    builder.append(prefix)
    builder.append(str)
    builder.append("\n")

    if (children.nonEmpty) {
      var count = 0
      children.init.foreach { child =>
        child.generateTreeString(
          depth + 1,
          lastChildren :+ false,
          builder,
          verbose,
          s"[$count] ",
          addSuffix
        )
        count += 1
      }
      children.last.generateTreeString(
        depth + 1,
        lastChildren :+ true,
        builder,
        verbose,
        s"[$count] ",
        addSuffix
      )
    }

    builder
  }
}
