import kotlinx.atomicfu.AtomicIntArray
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

/**
 * Int-to-Int hash map with open addressing and linear probes.
 *
 * TODO: This class is **NOT** thread-safe.
 */
class IntIntHashMap {
    private val core = atomic(Core(INITIAL_CAPACITY))

    /**
     * Returns value for the corresponding key or zero if this key is not present.
     *
     * @param key a positive key.
     * @return value for the corresponding or zero if this key is not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    operator fun get(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        val currentCore = core.value
        return toValue(currentCore.getInternal(key))
    }

    /**
     * Changes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key   a positive key.
     * @param value a positive value.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key or value are not positive, or value is equal to
     * [Integer.MAX_VALUE] which is reserved.
     */
    fun put(key: Int, value: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        require(isValue(value)) { "Invalid value: $value" }
        return toValue(putAndRehashWhileNeeded(key, value))
    }

    /**
     * Removes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key a positive key.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    fun remove(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(putAndRehashWhileNeeded(key, DEL_VALUE))
    }

    private fun putAndRehashWhileNeeded(key: Int, value: Int): Int {
        while (true) {
            val currentCore = core.value
            val oldValue = currentCore.putInternal(key, value)
            if (oldValue != NEEDS_REHASH) return oldValue
            core.compareAndSet(currentCore, currentCore.rehash())
        }
    }

    private class Core internal constructor(capacity: Int) {
        // Pairs of <key, value> here, the actual
        // size of the map is twice as big.
        val map = AtomicIntArray(2 * capacity)
        val shift: Int
        val next: AtomicRef<Core?> = atomic(null)

        init {
            val mask = capacity - 1
            assert(mask > 0 && mask and capacity == 0) { "Capacity must be power of 2: $capacity" }
            shift = 32 - Integer.bitCount(mask)
        }

        fun getInternal(key: Int): Int {
            var index = index(key)
            var probes = 0
            while (true) {
                while (map[index].value != key) { // optimize for successful lookup
                    if (map[index].value == NULL_KEY) return NULL_VALUE // not found -- no value
                    if (++probes >= MAX_PROBES) return NULL_VALUE
                    if (index == 0) index = map.size
                    index -= 2
                }
                // found key -- return value
                val currentValue = map[index + 1].value
                val nextCore = next.value
                when (currentValue) {
                    MOVED_VALUE -> if (nextCore !== null)
                        return nextCore.getInternal(key) else throw IllegalArgumentException("Something wrong")

                    in (MOVED_VALUE until 0) -> {
                        val fixedValue = -currentValue
                        if (nextCore !== null) {
                            nextCore.move(map[index].value, fixedValue)
                            val result = nextCore.getInternal(key)
                            map[index + 1].compareAndSet(currentValue, MOVED_VALUE)
                            return result
                        } else throw IllegalArgumentException("Something wrong")
                    }

                    in (0..DEL_VALUE) -> return currentValue
                    DEL_VALUE -> return currentValue
                }
            }
        }

        fun putInternal(key: Int, value: Int): Int {
            var index = index(key)
            var probes = 0
            while (true) { // optimize for successful lookup
                val currentKey = map[index].value
                val currentValue = map[index + 1].value
                val nextCore = next.value
                if (currentKey == NULL_KEY) {
                    // not found -- claim this slot
                    if (value == DEL_VALUE) return NULL_VALUE // remove of missing item, no need to claim slot
                    if (map[index].compareAndSet(NULL_KEY, key)) {
                        if (map[index + 1].compareAndSet(currentValue, value)) return currentValue else continue
                    } else continue
                }
                if (currentKey == key) {
                    // found key -- update value
                    when (currentValue) {
                        MOVED_VALUE -> if (nextCore !== null)
                            return nextCore.putInternal(
                                key,
                                value
                            ) else throw IllegalArgumentException("Something wrong")

                        in (MOVED_VALUE until 0) -> {
                            val fixedValue = -currentValue
                            if (nextCore !== null) {
                                nextCore.move(map[index].value, fixedValue)
                                val result = nextCore.putInternal(key, value)
                                map[index + 1].compareAndSet(currentValue, MOVED_VALUE)
                                return result
                            } else throw IllegalArgumentException("Something wrong")
                        }

                        in (0..DEL_VALUE) -> if (map[index + 1].compareAndSet(currentValue, value))
                            return currentValue else continue
                    }
                }
                if (++probes >= MAX_PROBES) return if (value == DEL_VALUE) NULL_VALUE else NEEDS_REHASH
                if (index == 0) index = map.size
                index -= 2
            }
        }

        fun rehash(): Core {
            next.compareAndSet(null, Core(map.size)) // map.length is twice the current capacity
            val newCore = next.value ?: throw IllegalArgumentException("Something wrong")
            var index = 0
            while (index < map.size) {
                while (true) {
                    val currentValue = map[index + 1].value
                    val currentKey = map[index].value
                    when (currentValue) {
                        MOVED_VALUE -> break
                        in (MOVED_VALUE until 0) -> {
                            val fixedValue = -currentValue
                            newCore.move(currentKey, fixedValue)
                            if (map[index + 1].compareAndSet(currentValue, MOVED_VALUE)) break
                        }

                        in (0..DEL_VALUE) -> {
                            val fixedValue = -currentValue
                            if (map[index + 1].compareAndSet(currentValue, fixedValue)) {
                                newCore.move(currentKey, currentValue)
                                if (map[index + 1].compareAndSet(fixedValue, MOVED_VALUE)) break
                            }
                        }

                        DEL_VALUE -> if (map[index + 1].compareAndSet(currentValue, MOVED_VALUE)) break
                    }
                }
                index += 2
            }
            return newCore
        }

        /**
         * Returns an initial index in map to look for a given key.
         */
        fun index(key: Int): Int = (key * MAGIC ushr shift) * 2

        private fun move(key: Int, value: Int) {
            var index = index(key)
            var probes = 0
            while (true) {
                val currentKey = map[index].value
                if (currentKey == NULL_KEY) {
                    if (map[index].compareAndSet(NULL_KEY, key))
                        if (map[index + 1].compareAndSet(NULL_VALUE, value))
                            return else continue else continue
                }
                if (currentKey == key) {
                    map[index + 1].compareAndSet(NULL_VALUE, value)
                    return
                }
                if (++probes >= MAX_PROBES) return
                if (index == 0) index = map.size
                index -= 2
            }
        }
    }
}

private const val MAGIC = -0x61c88647 // golden ratio
private const val INITIAL_CAPACITY = 2 // !!! DO NOT CHANGE INITIAL CAPACITY !!!
private const val MAX_PROBES = 8 // max number of probes to find an item
private const val NULL_KEY = 0 // missing key (initial value)
private const val NULL_VALUE = 0 // missing value (initial value)
private const val DEL_VALUE = Int.MAX_VALUE // mark for removed value
private const val MOVED_VALUE = Int.MIN_VALUE
private const val NEEDS_REHASH = -1 // returned by `putInternal` to indicate that rehash is needed

// Checks is the value is in the range of allowed values
private fun isValue(value: Int): Boolean = value in (1 until DEL_VALUE)

// Converts internal value to the public results of the methods
private fun toValue(value: Int): Int = if (isValue(value)) value else 0