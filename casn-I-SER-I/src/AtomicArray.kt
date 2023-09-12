import kotlinx.atomicfu.*

class AtomicArray<E>(size: Int, initialValue: E) {
    private val a = atomicArrayOfNulls<Ref<E>>(size)

    init {
        for (i in 0 until size) a[i].value = Ref(initialValue)
    }

    fun get(index: Int) = a[index].value!!.value

    fun set(index: Int, value: E) {
        a[index].value!!.value = value
    }

    fun cas(index: Int, expected: E, update: E) = a[index].value!!.cas(expected, update)

    fun cas2(
        index1: Int, expected1: E, update1: E,
        index2: Int, expected2: E, update2: E
    ): Boolean {
        while (true) {
            if (index1 == index2) {
                return if (expected1 == expected2) cas(index1, expected1, update2) else false
            }

            val firstIndex = if (index1 <= index2) index1 else index2
            val firstExpected = if (index1 <= index2) expected1 else expected2
            val cas2Descriptor =
                if (index1 <= index2)
                    CAS2Descriptor(a[index1].value!!, expected1, update1, a[index2].value!!, expected2, update2)
                else
                    CAS2Descriptor(a[index2].value!!, expected2, update2, a[index1].value!!, expected1, update1)

            if (a[firstIndex].value!!.cas(firstExpected, cas2Descriptor)) { // 1 to 2
                cas2Descriptor.complete()
                return cas2Descriptor.outcome.value === Outcome.SUCCESS
            } else if (a[firstIndex].value!!.value != firstExpected) return false // 1 to 7
        }
    }

    class Ref<TValue>(initial: TValue) {
        val reference = atomic<Any?>(initial)

        var value: TValue
            get() {
                reference.loop { currentValue ->
                    when (currentValue) {
                        is Descriptor -> currentValue.complete()
                        else -> return currentValue as TValue
                    }
                }
            }
            set(updateValue) {
                reference.loop { currentValue ->
                    when (currentValue) {
                        is Descriptor -> currentValue.complete()
                        else -> if (reference.compareAndSet(currentValue, updateValue)) return
                    }
                }
            }

        fun cas(expect: Any?, update: Any?): Boolean {
            reference.loop { currentValue ->
                when {
                    currentValue is Descriptor -> currentValue.complete()
                    currentValue === expect -> if (reference.compareAndSet(expect, update)) return true
                    else -> return false
                }
            }
        }
    }

    private class RDCSSDescriptor<AValue, BValue>(
        val a: Ref<AValue>, val expectA: Any?, val updateA: Any?,
        val b: Ref<BValue>, val expectB: Any?
    ) : Descriptor() {
        override fun complete() { // 4
            if (b.value === expectB) outcome.reference.compareAndSet(Outcome.UNDECIDED, Outcome.SUCCESS)
            else outcome.reference.compareAndSet(Outcome.UNDECIDED, Outcome.FAIL)
            val updateValue = if (outcome.value === Outcome.SUCCESS) updateA else expectA
            a.reference.compareAndSet(this, updateValue)
        }
    }

    private class CAS2Descriptor<AValue, BValue>(
        private val a: Ref<AValue>, private val expectA: AValue, private val updateA: AValue,
        private val b: Ref<BValue>, private val expectB: BValue, private val updateB: BValue,
    ) : Descriptor() {
        override fun complete() {
            if (b.reference.value !== this) {
                val rdcssDescriptor = RDCSSDescriptor(b, expectB, this, outcome, Outcome.UNDECIDED)
                if (b.cas(expectB, rdcssDescriptor)) { // 2 to 3
                    rdcssDescriptor.complete()
                    outcome.reference.compareAndSet(Outcome.UNDECIDED, Outcome.SUCCESS)
                } else outcome.reference.compareAndSet(Outcome.UNDECIDED, Outcome.FAIL)
            } else outcome.reference.compareAndSet(Outcome.UNDECIDED, Outcome.SUCCESS)

            if (outcome.value === Outcome.SUCCESS) { // 5 to 6
                a.reference.compareAndSet(this, updateA)
                b.reference.compareAndSet(this, updateB)
            } else { // 8 to 9
                a.reference.compareAndSet(this, expectA)
                b.reference.compareAndSet(this, expectB)
            }
        }
    }

    private abstract class Descriptor {
        val outcome = Ref(Outcome.UNDECIDED)
        abstract fun complete()
    }

    private enum class Outcome {
        UNDECIDED, SUCCESS, FAIL
    }
}