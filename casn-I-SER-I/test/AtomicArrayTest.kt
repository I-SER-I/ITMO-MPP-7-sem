import com.amazonaws.auth.*
import com.amazonaws.regions.*
import com.amazonaws.services.s3.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.strategy.stress.*
import org.jetbrains.kotlinx.lincheck.verifier.*
import org.junit.*
import java.io.*

@Param.Params(
    Param(name = "index", gen = IntGen::class, conf = "0:4"),
    Param(name = "value", gen = IntGen::class, conf = "0:2")
)
class AtomicArrayTest {
    private val a = AtomicArray(5, 0)

    @Operation(params = ["index"])
    fun get(index: Int) =
        a.get(index)

    @Operation(params = ["index", "value", "value"])
    fun cas(index: Int, expected: Int, update: Int) =
        a.cas(index, expected, update)

    @Operation(params = ["index", "value", "value",
                         "index", "value", "value"])
    fun cas2(index1: Int, expected1: Int, update1: Int,
             index2: Int, expected2: Int, update2: Int) =
        a.cas2(index1, expected1, update1, index2, expected2, update2)

    @Test
    fun stressTest() = try {
        StressOptions()
            .iterations(100)
            .invocationsPerIteration(10_000)
            .actorsBefore(0)
            .actorsAfter(0)
            .threads(3)
            .actorsPerThread(3)
            .sequentialSpecification(AtomicArrayIntSequential::class.java)
            .check(this::class.java)
    } catch (t: Throwable) {
        uploadWrongSolutionToS3("stress")
        throw t
    }

    @Test
    fun modelCheckingTest() = try {
        ModelCheckingOptions()
            .iterations(100)
            .invocationsPerIteration(10_000)
            .actorsBefore(0)
            .actorsAfter(0)
            .threads(2)
            .actorsPerThread(3)
            .sequentialSpecification(AtomicArrayIntSequential::class.java)
            .checkObstructionFreedom(true)
            .addCustomScenario {
                parallel {
                    thread {
                        actor(AtomicArrayTest::cas2, 0, 0, 1, 1, 0, 1)
                    }
                    thread {
                        actor(AtomicArrayTest::get, 0)
                        actor(AtomicArrayTest::cas, 1, 1, 0)
                        actor(AtomicArrayTest::get, 1)
                    }
                }
            }
            .check(this::class.java)
    } catch (t: Throwable) {
        uploadWrongSolutionToS3("model-checking")
        throw t
    }
}

class AtomicArrayIntSequential : VerifierState() {
    private val a = arrayOfNulls<Int>(5)

    init {
        for (i in 0 until 5) a[i] = 0
    }

    fun get(index: Int): Int? = a[index]

    fun set(index: Int, value: Int) {
        a[index] = value
    }

    fun cas(index: Int, expected: Int, update: Int): Boolean {
        if (a[index] != expected) return false
        a[index] = update
        return true
    }

    fun cas2(index1: Int, expected1: Int, update1: Int,
             index2: Int, expected2: Int, update2: Int): Boolean {
        if (a[index1] != expected1 || a[index2] != expected2) return false
        a[index1] = update1
        a[index2] = update2
        return true
    }

    override fun extractState() = a.toList()
}

private fun uploadWrongSolutionToS3(strategy: String) = runCatching {
    val solutionFile = File("src/$TASK_NAME.kt")
    val date = java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm").format(java.util.Date())
    val destinationFileLocation = "$YEAR/$TASK_NAME/$TASK_NAME-$date-$strategy-${kotlin.random.Random.nextInt(1000)}.kt"
    val credentials = BasicAWSCredentials("AKIA27OSP7CB7EEHHOX7", "iyFzeiqHS0amZQj79Jh1DNMy+s96f+fcJvy+BHQu")
    val s3client = AmazonS3ClientBuilder.standard()
        .withCredentials(AWSStaticCredentialsProvider(credentials))
        .withRegion(Regions.US_EAST_2)
        .build()
    s3client.putObject(S3_BUCKET_NAME, destinationFileLocation, solutionFile)
}.let {
    if (it.isFailure) {
        System.err.println("INCORRECT IMPLEMENTATION UPLOADING HAS FAILED, PLEASE CONTACT NIKITA KOVAL TO FIX THE ISSUE")
        it.exceptionOrNull()!!.printStackTrace()
    }
}

private val YEAR = "2022"
private val TASK_NAME = "AtomicArray"
private val S3_BUCKET_NAME = "mpp2022incorrectimplementations"
