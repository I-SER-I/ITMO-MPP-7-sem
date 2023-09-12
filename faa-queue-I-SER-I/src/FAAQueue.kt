package mpp.faaqueue

import kotlinx.atomicfu.*

class FAAQueue<E> {
    private val head: AtomicRef<Segment> // Head pointer, similarly to the Michael-Scott queue (but the first node is _not_ sentinel)
    private val tail: AtomicRef<Segment> // Tail pointer, similarly to the Michael-Scott queue
    private val enqIdx = atomic(0L)
    private val deqIdx = atomic(0L)

    init {
        val firstNode = Segment(0)
        head = atomic(firstNode)
        tail = atomic(firstNode)
    }

    /**
     * Adds the specified element [x] to the queue.
     */
    fun enqueue(element: E) {
        while (true) {
            val currentTail = tail.value
            val index = enqIdx.getAndIncrement()
            val segment = findSegment(currentTail, index / SEGMENT_SIZE)
            moveTailForward(segment)
            val elementIndex = (index % SEGMENT_SIZE).toInt()
            if (segment.cas(elementIndex, null, element)) return
        }
    }

    /**
     * Retrieves the first element from the queue and returns it;
     * returns `null` if the queue is empty.
     */
    fun dequeue(): E? {
        while (true) {
            if (isEmpty) return null
            val currentHead = head.value
            val index = deqIdx.getAndIncrement()
            val segment = findSegment(currentHead, index / SEGMENT_SIZE)
            moveHeadForward(segment)
            val elementIndex = (index % SEGMENT_SIZE).toInt()
            if (segment.cas(elementIndex, null, Any())) continue
            return segment.get(elementIndex) as E?
        }
    }

    private fun findSegment(start: Segment, id: Long): Segment {
        var segment = start
        while (segment.id < id) {
            if (segment.next.value == null) {
                segment.next.compareAndSet(null, Segment(segment.id + 1))
            } else {
                segment = segment.next.value!!
            }
        }
        return segment
    }

    private fun moveTailForward(segment: Segment) {
        while (true) {
            val currentTail = tail.value
            if (currentTail.id >= segment.id) return
            if (tail.compareAndSet(currentTail, segment)) return
        }
    }

    private fun moveHeadForward(segment: Segment) {
        while (true) {
            val currentHead = head.value
            if (currentHead.id >= segment.id) return
            if (head.compareAndSet(currentHead, segment)) return
        }
    }

    /**
     * Returns `true` if this queue is empty, or `false` otherwise.
     */
    val isEmpty: Boolean
        get() {
            return deqIdx.value >= enqIdx.value
        }
}

private class Segment(val id: Long) {
    val next: AtomicRef<Segment?> = atomic(null)
    val elements = atomicArrayOfNulls<Any>(SEGMENT_SIZE)

    fun get(i: Int) = elements[i].value
    fun cas(i: Int, expect: Any?, update: Any?) = elements[i].compareAndSet(expect, update)
    private fun put(i: Int, value: Any?) {
        elements[i].value = value
    }
}

const val SEGMENT_SIZE = 2 // DO NOT CHANGE, IMPORTANT FOR TESTS

