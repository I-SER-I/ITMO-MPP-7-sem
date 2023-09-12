package mpp.dynamicarray

import kotlinx.atomicfu.*

interface DynamicArray<E> {
    /**
     * Returns the element located in the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun get(index: Int): E

    /**
     * Puts the specified [element] into the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun put(index: Int, element: E)

    /**
     * Adds the specified [element] to this array
     * increasing its [size].
     */
    fun pushBack(element: E)

    /**
     * Returns the current size of this array,
     * it increases with [pushBack] invocations.
     */
    val size: Int
}

class DynamicArrayImpl<E> : DynamicArray<E> {
    private val core = atomic(Core<E>(INITIAL_CAPACITY))
    private val _size = atomic(0)

    override fun get(index: Int): E {
        require(index < _size.value)
        return core.value.get(index).element
    }

    override fun put(index: Int, element: E) {
        require(index < _size.value)
        while (true) {
            val currentCore = core.value
            val cell = currentCore.get(index)
            if (cell is Moved) {
                move(currentCore)
                continue
            }
            if (currentCore.cas(index, cell, Based(element))) return
        }
    }

    override fun pushBack(element: E) {
        while (true) {
            val currentCore = core.value
            val currentSize = _size.value
            if (currentSize >= currentCore.capacity) {
                move(currentCore)
                continue
            }
            if (currentCore.cas(currentSize, null, Based(element))) {
                _size.compareAndSet(currentSize, currentSize + 1)
                return
            }
            _size.compareAndSet(currentSize, currentSize + 1)
        }
    }

    private fun move(currentCore: Core<E>) {
        currentCore.next.compareAndSet(null, Core(currentCore.capacity * 2))
        val nextCore = currentCore.next.value!!
        for (i in 0 until currentCore.capacity) {
            while (true) {
                val cell = currentCore.get(i)
                if (currentCore.cas(i, cell, Moved(cell.element))) {
                    nextCore.cas(i, null, Based(cell.element))
                    break
                }
            }
        }
        core.compareAndSet(currentCore, nextCore)
    }

    override val size: Int get() = _size.value
}

private class Core<E>(
    val capacity: Int,
) {
    private val array = atomicArrayOfNulls<Cell<E>>(capacity)
    val next = atomic<Core<E>?>(null)

    @Suppress("UNCHECKED_CAST")
    fun get(index: Int): Cell<E> = array[index].value as Cell<E>
    fun cas(index: Int, expected: Cell<E>?, updated: Cell<E>?): Boolean =
        array[index].compareAndSet(expected, updated)
}

private open class Cell<E>(val element: E)
private class Moved<E>(element: E) : Cell<E>(element)
private class Based<E>(element: E) : Cell<E>(element)

private const val INITIAL_CAPACITY = 1 // DO NOT CHANGE ME