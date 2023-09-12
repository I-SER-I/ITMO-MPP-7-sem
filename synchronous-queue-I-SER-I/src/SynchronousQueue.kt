import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

/**
 * An element is transferred from sender to receiver only when [send] and [receive]
 * invocations meet in time (rendezvous), so [send] suspends until another coroutine
 * invokes [receive] and [receive] suspends until another coroutine invokes [send].
 */
class SynchronousQueue<E> {
    private val head: AtomicRef<Node<E>>
    private val tail: AtomicRef<Node<E>>

    private object RETRY

    init {
        val dummy = Node<E>(null, false, null)
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    /**
     * Sends the specified [element] to this channel, suspending if there is no waiting
     * [receive] invocation on this channel.
     */
    suspend fun send(element: E) {
        while (true) {
            val currentHead = head.value
            val currentTail = tail.value
            val value = if (currentHead == currentTail || currentTail.element != null)
                enqueueAndSuspend(currentTail, element) else dequeueAndResume(currentHead, element)
            if (value != RETRY) break
        }
    }

    /**
     * Retrieves and removes an element from this channel if there is a waiting [send] invocation on it,
     * suspends the caller if this channel is empty.
     */
    suspend fun receive(): E {
        while (true) {
            val currentHead = head.value
            val currentTail = tail.value
            val value = if (currentHead == currentTail || currentTail.element == null)
                enqueueAndSuspend(currentTail, null) else dequeueAndResume(currentHead, null)
            if (value != RETRY) return value as E
        }
    }

    private suspend fun enqueueAndSuspend(currentTail: Node<E>, element: E?): Any? {
        return suspendCoroutine suspendCoroutine@{ continuation ->
            val newTail = Node(element, currentTail.isSender, continuation)
            val retry = !currentTail.next.compareAndSet(null, newTail)
            tail.compareAndSet(currentTail, currentTail.next.value!!)
            if (retry) {
                continuation.resume(RETRY)
                return@suspendCoroutine
            }
        }
    }

    private fun dequeueAndResume(currentHead: Node<E>, element: E?): Any? {
        val newHead = currentHead.next.value!!
        return if (head.compareAndSet(currentHead, newHead)) {
            newHead.continuation!!.resume(element)
            newHead.element
        } else RETRY
    }
}

private class Node<E>(val element: E?, val isSender: Boolean, val continuation: Continuation<E?>?) {

    val next = atomic<Node<E>?>(null)
}