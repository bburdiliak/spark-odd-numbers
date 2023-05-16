import model.{Algorithm, AppConfig}
import model.AppConfig.validate
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.funsuite.AnyFunSuite

class AppConfigTest extends AnyFunSuite with BeforeAndAfterAll with PrivateMethodTester {

  test("fails with 0 arguments") {
    assert(validate(Array[String]()).swap.map(_.contains("0 arguments given")).contains(true))
  }

  test("fails with 1 argument") {
    assert(validate(Array("")).swap.map(_.contains("1 argument given")).contains(true))
  }

  test("fails with 2 arguments") {
    assert(validate(Array("", "")).swap.map(_.contains("2 arguments given")).contains(true))
  }

  test("with 3 arguments, fails when invalid algorithm") {
    List("-1", "0", "4", "anything").foreach { invalidAlgorithm =>
      val withInvalidAlgorithm = Array("src/main/resources/input/text01.txt", "src/main/resources/output-here.csv", invalidAlgorithm)
      assert(validate(withInvalidAlgorithm).swap.contains("Invalid algorithm"))
    }
  }

  test("with 3 arguments, fails when invalid output path") {
    val withInvalidAlgorithm = Array("src/main/resources/input/text01.txt", "\"\u0000/-1.txt", "1")
    assert(validate(withInvalidAlgorithm).swap.map(_.contains("Nul character not allowed")).contains(true))
  }

  test("succeeds with 3 valid arguments") {
    val withInvalidAlgorithm = Array("src/main/resources/input/text01.txt", "src/main/resources/output/text01.txt", "1")
    validate(withInvalidAlgorithm) match {
      case Left(_) => fail("Should not fail")
      case Right(appConfig) => {
        assert(appConfig.inputPath.toString === "src/main/resources/input/text01.txt")
        assert(appConfig.output.toString === "src/main/resources/output/text01.txt")
        assert(appConfig.algorithm === Algorithm.Xor)
      }
    }
  }

}
