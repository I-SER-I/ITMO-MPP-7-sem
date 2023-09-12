import java.util.concurrent.atomic.*

class Solution(val env: Environment) : Lock<Solution.Node> {
    private val tail = AtomicReference<Node>(null)

    override fun lock(): Node {
        val my = Node() // сделали узел
        val predecessor = tail.getAndSet(my)
        if (predecessor !== null) {
            my.locked.set(true)
            predecessor.next.value = my
            while (my.locked.get()) env.park()
        }
        return my // вернули узел
    }

    override fun unlock(node: Node) {
        if (node.next.value === null) {
            if (tail.compareAndSet(node, null)) return
            while (node.next.value === null) continue
        }
        node.next.value.locked.set(false)
        env.unpark(node.next.value.thread)
    }

    class Node {
        val thread = Thread.currentThread() // запоминаем поток, которые создал узел
        val locked = AtomicReference(false)
        val next = AtomicReference<Node>(null)
    }
}