package dijkstra

import kotlinx.atomicfu.locks.ReentrantLock
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val multiQueue = MultiPriorityQueue(workers, NODE_DISTANCE_COMPARATOR)
    multiQueue.insert(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val activeNodes = AtomicInteger(1)
    repeat(workers) {
        thread {
            while (activeNodes.get() > 0) {
                val currentNode: Node = multiQueue.delete() ?: if (activeNodes.get() == 0) break else continue
                for (edge in currentNode.outgoingEdges) {
                    while(true) {
                        val currentDistance = edge.to.distance
                        val newDistance = currentNode.distance + edge.weight
                        if (currentDistance > newDistance) {
                            if (edge.to.casDistance(currentDistance, newDistance)) {
                                activeNodes.incrementAndGet()
                                multiQueue.insert(edge.to)
                                break
                            }
                        } else {
                            break
                        }
                    }
                }
                activeNodes.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

class MultiPriorityQueue<TValue>(queueCount: Int, private val comparator: Comparator<TValue>) {
    private val queues = List(queueCount) { PriorityQueue(comparator) }
    private val queueLocks = List(queueCount) { ReentrantLock() }
    fun insert(value: TValue) {
        while (true) {
            val index = Random.nextInt(queues.size)
            if (queueLocks[index].tryLock()) {
                queues[index].add(value)
                queueLocks[index].unlock()
                return
            }
        }
    }

    fun delete(): TValue? {
        while (true) {
            val firstIndex = Random.nextInt(queues.size)
            val secondIndex = Random.nextInt(queues.size)
            if (firstIndex == secondIndex) continue
            if (queueLocks[firstIndex].tryLock()) {
                if (queueLocks[secondIndex].tryLock()) {
                    val firstNode = queues[firstIndex].peek()
                    val secondNode = queues[secondIndex].peek()
                    val node = when {
                        firstNode === null && secondNode === null -> null
                        firstNode !== null && secondNode === null -> queues[firstIndex].poll()
                        firstNode === null && secondNode !== null -> queues[secondIndex].poll()
                        else -> if (comparator.compare(firstNode, secondNode) < 0) queues[firstIndex].poll() else queues[secondIndex].poll()
                    }
                    queueLocks[firstIndex].unlock()
                    queueLocks[secondIndex].unlock()
                    return node
                }
                queueLocks[firstIndex].unlock()
            }
        }
    }
}