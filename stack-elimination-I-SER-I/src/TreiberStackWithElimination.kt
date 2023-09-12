package mpp.stackWithElimination

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import kotlin.random.Random

class TreiberStackWithElimination<E> {
    private val top = atomic<Node<E>?>(null)
    private val eliminationArray = atomicArrayOfNulls<Any?>(ELIMINATION_ARRAY_SIZE)

    /**
     * Adds the specified element [x] to the stack.
     */
    fun push(x: E) {
        val randomIndex = Random.nextInt(ELIMINATION_ARRAY_SIZE);
        while (true) {
            if (tryPush(x)) {
                return
            }
            if (eliminationArray[randomIndex].compareAndSet(null, x)) {
                repeat(1000) {}
                if (eliminationArray[randomIndex].compareAndSet(x, null)) {
                    return
                }
            }
        }
    }

    /**
     * Retrieves the first element from the stack
     * and returns it; returns `null` if the stack
     * is empty.
     */
    fun pop(): E? {
        val randomIndex = Random.nextInt(ELIMINATION_ARRAY_SIZE)
        while (true) {
            val x = tryPop()
            if (x != null) {
                return x
            } else {
                val x = eliminationArray[randomIndex].value
                if (eliminationArray[randomIndex].compareAndSet(x, null)) {
                    return x as E
                }
            }
        }
    }

    private fun tryPush(x: E): Boolean {
        while (true) {
            val currentTop = top.value
            val newTop = Node(x, currentTop)
            if (top.compareAndSet(currentTop, newTop)) {
                return true
            }
        }
    }

    private fun tryPop(): E? {
        while (true) {
            val currentTop = top.value ?: return null
            val newTop = currentTop.next
            if (top.compareAndSet(currentTop, newTop)) {
                return currentTop.x
            }
        }
    }
}

private class Node<E>(val x: E, val next: Node<E>?)

private const val ELIMINATION_ARRAY_SIZE = 2 // DO NOT CHANGE IT