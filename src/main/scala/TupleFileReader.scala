import org.apache.spark.sql.SparkSession

object TupleFileReader {

  private val HeaderCommaPattern = """\s*(.*)\s*,\s*(.*)\s*""".r
  private val HeaderTabPattern = """\s*(.*)\t\s*(.*)\s*""".r
  private val CommaPattern = """\s*(\d+),\s*(\d+)\s*""".r
  private val TabPattern = """\s*(\d+)\t\s*(\d+)\s*""".r

  def read(inputFile: String)(implicit spark: SparkSession) =
    spark.sparkContext.textFile(inputFile).map {
      case CommaPattern(num1, num2) => Some((num1.toInt, num2.toInt))
      case TabPattern(num1, num2) => Some((num1.toInt, num2.toInt))
      case HeaderTabPattern(_, _) => None
      case HeaderCommaPattern(_, _) => None
    }.collect { case Some(x) => x }

}
