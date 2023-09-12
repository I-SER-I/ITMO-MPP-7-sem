package mpp.linkedlistset

import kotlinx.atomicfu.*

class LinkedListSet<E : Comparable<E>> {
    private val first = Node<E>(prev = null, element = null, next = null)
    private val last = Node<E>(prev = first, element = null, next = null)

    init {
        first.setNext(last)
    }

    private val head = atomic(first)

    /**
     * Adds the specified element to this set
     * if it is not already present.
     *
     * Returns `true` if this set did not
     * already contain the specified element.
     */
    fun add(element: E): Boolean {
        while (true) {
            val windowNode = getWindowNode(element)
            if (windowNode.element == element) return false

            var previousNode = windowNode.prev!!
            while (previousNode.next!!.element != null && previousNode.next!!.element!! < element)
                previousNode = previousNode.next!!

            val newNode = Node(previousNode, element, windowNode)
            if (previousNode.casNext(windowNode, newNode)) {
                windowNode.casPrev(windowNode, newNode)
                return true
            }
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
        while (true) {
            val windowNode = getWindowNode(element)
            if (windowNode.element !== element) return false
            if (windowNode.isRemoved.compareAndSet(windowNode.isRemoved.value, true)) {
                windowNode.prev!!.casNext(windowNode, windowNode.next)
                windowNode.next!!.casPrev(windowNode, windowNode.prev)
                return true
            }
        }
    }

    /**
     * Returns `true` if this set contains
     * the specified element.
     */
    fun contains(element: E): Boolean = getWindowNode(element).element == element

    private fun getWindowNode(element: E): Node<E> {
        var currentNode = head.value.next!!
        while (currentNode.element != null && currentNode.element!! < element) {
            currentNode = currentNode.next!!
        }
        return currentNode
    }
}

private class Node<E : Comparable<E>>(prev: Node<E>?, element: E?, next: Node<E>?) {
    private val _element = element // `null` for the first and the last nodes
    val element get() = _element
    val isRemoved = atomic(false)

    private val _prev = atomic(prev)
    val prev get() = _prev.value
    fun setPrev(value: Node<E>?) {
        _prev.value = value
    }

    fun casPrev(expected: Node<E>?, update: Node<E>?) =
        _prev.compareAndSet(expected, update)

    private val _next = atomic(next)
    val next get() = _next.value
    fun setNext(value: Node<E>?) {
        _next.value = value
    }

    fun casNext(expected: Node<E>?, update: Node<E>?) =
        _next.compareAndSet(expected, update)
}