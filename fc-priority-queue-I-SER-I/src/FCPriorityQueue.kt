import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import java.util.*
import java.util.concurrent.ThreadLocalRandom

class FCPriorityQueue<E : Comparable<E>> {
    private val q = PriorityQueue<E>()
    private val lock = atomic(false)
    private val flatCombiningArray = atomicArrayOfNulls<Operation<E>?>(16)

    /**
     * Retrieves the element with the highest priority
     * and returns it as the result of this function;
     * returns `null` if the queue is empty.
     */
    fun poll(): E? {
        return flatCombining(Operation(OperationType.POLL))
    }

    /**
     * Returns the element with the highest priority
     * or `null` if the queue is empty.
     */
    fun peek(): E? {
        return flatCombining(Operation(OperationType.PEEK))
    }

    /**
     * Adds the specified element to the queue.
     */
    fun add(element: E) {
        flatCombining(Operation(OperationType.ADD, element))
    }

    private fun flatCombining(operation: Operation<E>): E? {
        var index = ThreadLocalRandom.current().nextInt(flatCombiningArray.size)
        while (true) {
            if (tryLock()) {
                val operationResult = executeOperation(operation)
                support()
                unlock()
                return operationResult
            } else {
                while (!flatCombiningArray[index].compareAndSet(null, operation)) {
                    index = ThreadLocalRandom.current().nextInt(flatCombiningArray.size)
                }
                while (true) {
                    val value = flatCombiningArray[index].value
                    if (value !== null) {
                        if (value.status === OperationType.DONE) {
                            flatCombiningArray[index].compareAndSet(operation, null)
                            return value.element
                        }
                    }
                    if (tryLock()) {
                        val message = flatCombiningArray[index].value
                        flatCombiningArray[index].compareAndSet(operation, null)
                        if (message !== null) {
                            return if (message.status === OperationType.DONE) {
                                unlock()
                                message.element
                            } else {
                                val operationResult = executeOperation(operation)
                                support()
                                unlock()
                                operationResult
                            }
                        }
                    }
                }
            }
        }
    }

    private fun executeOperation(operation: Operation<E>): E? = when (operation.status) {
        OperationType.POLL -> q.poll()
        OperationType.PEEK -> q.peek()
        OperationType.ADD -> q.add(operation.element).let { null }
        OperationType.DONE -> null
    }

    private fun support() {
        for (i in 0 until flatCombiningArray.size) {
            val operation = flatCombiningArray[i].value ?: continue
            when (operation.status) {
                OperationType.POLL -> {
                    val result = q.poll()
                    flatCombiningArray[i].compareAndSet(operation, Operation(OperationType.DONE, result))
                }

                OperationType.PEEK -> {
                    val result = q.peek()
                    flatCombiningArray[i].compareAndSet(operation, Operation(OperationType.DONE, result))
                }

                OperationType.ADD -> {
                    q.add(operation.element)
                    flatCombiningArray[i].compareAndSet(operation, Operation(OperationType.DONE))
                }

                OperationType.DONE -> continue
            }
        }
    }

    private fun tryLock() = lock.compareAndSet(false, true)

    private fun unlock() = lock.compareAndSet(true, false)

    private enum class OperationType {
        POLL, PEEK, ADD, DONE
    }

    private data class Operation<E>(val status: OperationType, val element: E? = null)
}