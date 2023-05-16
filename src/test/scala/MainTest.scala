import model.Algorithm._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}

class MainTest extends AnyFunSuite with BeforeAndAfterAll with PrivateMethodTester {

  private var sparkConf: SparkConf = _
  private var session: SparkSession = _
  private var rdd: RDD[(Int, Int)] = _

  override def beforeAll() {
    sparkConf = new SparkConf().setAppName("unit-testing").setMaster("local")
    session = SparkSession.builder()
      .master("local[1]")
      .appName("OddNumberIdentifier")
      .getOrCreate()
    rdd = {
      println(session)
      TupleFileReader.read("src/test/resources/input/text01.txt")(session)
    }
  }

  def testOdds(odds: Map[Int, Int]) = {
    assert(odds.keySet.size === 5)
    assert(odds(1) === 2)
    assert(odds(2) === 4)
    assert(odds(3) === 1)
    assert(odds(4) === 2)
    assert(odds(5) === 1)
  }

  test("get odd numbers with algorithm1") {
    testOdds(Xor.run(rdd).collect().toMap)
  }

  test("get odd numbers with algorithm2") {
    testOdds(AggregateWithMap.run(rdd).collect().toMap)
  }

  test("get odd numbers with algorithm3") {
    testOdds(GroupAndFilter.run(rdd).collect().toMap)
  }

  override def afterAll() {
    session.sparkContext.stop()
  }


}
