package mpp.skiplist

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import java.util.concurrent.ThreadLocalRandom

class SkipList<E : Comparable<E>> {
    private val MAX_LEVEL = 32
    private val head: AtomicRef<Node<E>>
    private val tail: AtomicRef<Node<E>>

    init {
        head = atomic(Node(Int.MIN_VALUE as E, MAX_LEVEL))
        tail = atomic(Node(Int.MAX_VALUE as E, MAX_LEVEL))
        for (i in 0..MAX_LEVEL) head.value.next[i].value = tail.value
    }

    /**
     * Adds the specified element to this set
     * if it is not already present.
     *
     * Returns `true` if this set did not
     * already contain the specified element.
     */
    fun add(element: E): Boolean {
        val topLevel = ThreadLocalRandom.current().nextInt(MAX_LEVEL)
        val bottomLevel = 0
        val predecessors = arrayOfNulls<BasedNode<E>?>(MAX_LEVEL + 1)
        val successors = arrayOfNulls<BasedNode<E>?>(MAX_LEVEL + 1)
        while (true) {
            val found = find(element, predecessors, successors)
            if (found) return false
            val newNode = Node(element, topLevel)
            for (level in bottomLevel..topLevel) {
                val successor = successors[level]
                newNode.next[level].value = successor
            }
            val predecessor = predecessors[bottomLevel]
            val successor = successors[bottomLevel]
            val node = if (predecessor is Node<E>) predecessor else (predecessor as RemovedNode<E>).node
            if (node.next[bottomLevel].compareAndSet(successor, newNode)) {
                for (level in bottomLevel + 1..topLevel) {
                    while (true) {
                        val predecessor = predecessors[level]
                        val successor = successors[level]
                        val node = if (predecessor is Node<E>) predecessor else (predecessor as RemovedNode<E>).node
                        if (node.next[level].compareAndSet(successor, newNode)) break
                        find(element, predecessors, successors)
                    }
                }
            } else continue
            return true
        }
    }

    /**
     * Removes the specified element from this set
     * if it is present.
     *
     * Returns `true` if this set contained
     * the specified element.
     */
    fun remove(element: E): Boolean {
        val bottomLevel = 0
        val predecessors = arrayOfNulls<BasedNode<E>?>(MAX_LEVEL + 1)
        val successors = arrayOfNulls<BasedNode<E>?>(MAX_LEVEL + 1)
        while (true) {
            val found = find(element, predecessors, successors)
            if (!found) return false
            val nodeToRemove = successors[bottomLevel] as Node<E>
            for (level in nodeToRemove.height downTo bottomLevel + 1) {
                val successor = nodeToRemove.next[level].value
                if (successor is Node<E>) {
                    if (nodeToRemove.next[level].compareAndSet(successor, RemovedNode(successor))) break
                } else break
            }
            val successor = successors[bottomLevel]
            if (successor is Node<E>) {
                if (nodeToRemove.next[bottomLevel].compareAndSet(successor, RemovedNode(successor))) {
                    find(element, predecessors, successors)
                    return true
                }
            } else return false
        }
    }

    /**
     * Returns `true` if this set contains
     * the specified element.
     */
    fun contains(element: E): Boolean {
        val bottomLevel = 0
        var predecessor: BasedNode<E> = head.value
        var current: Node<E> = head.value
        while (true) {
            for (level in MAX_LEVEL downTo bottomLevel) {
                current = if (predecessor is Node<E>) predecessor else (predecessor as RemovedNode<E>).node
                while (true) {
                    var successor = current.next[level].value
                    while (successor is RemovedNode<E>) {
                        current = successor.node
                        successor = current.next[level].value
                    }
                    when {
                        current.element < element -> {
                            predecessor = current
                            current = successor as Node<E>
                        }

                        current.element == element -> return true
                        else -> break
                    }
                }
            }
            return current.element == element
        }
    }

    private fun find(
        element: E,
        predecessors: Array<BasedNode<E>?>,
        successors: Array<BasedNode<E>?>
    ): Boolean {
        val bottomLevel = 0
        var predecessor: BasedNode<E>? = null
        var current: BasedNode<E>? = null
        var successor: BasedNode<E>? = null
        retry@
        while (true) {
            predecessor = head.value
            for (level in MAX_LEVEL downTo bottomLevel) {
                current = (predecessor as Node<E>).next[level].value
                while (true) {
                    successor = (current as Node<E>).next[level].value
                    while (successor is RemovedNode<E>) {
                        if ((predecessor as Node<E>).next[level].compareAndSet(current, successor.node)) {
                            current = predecessor.next[level].value
                            successor = (current as Node<E>).next[level].value
                        } else continue@retry
                    }
                    val node = current as Node<E>
                    if (node.element < element) {
                        predecessor = node
                        current = successor
                    } else break
                }
                predecessors[level] = predecessor
                successors[level] = current
            }
            return (current as Node<E>).element == element
        }
    }
}

private interface BasedNode<E>
private class RemovedNode<E>(val node: Node<E>) : BasedNode<E>
private class Node<E>(val element: E, val height: Int) : BasedNode<E> {
    val next = atomicArrayOfNulls<BasedNode<E>?>(height + 1)
}