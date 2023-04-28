import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {

  def algorithm1(rddFromFile: RDD[(Int, Int)]) = rddFromFile.reduceByKey(_ ^ _)

  def algorithm2(rddFromFile: RDD[(Int, Int)]) =
    rddFromFile.aggregateByKey(Map.empty[Int, Boolean])(
      (acc, element) => if (acc.contains(element)) acc - element else acc + (element -> true),
      (acc1, acc2) => {
        val keep = acc1.keySet.diff(acc2.keySet) ++ acc2.keySet.diff(acc1.keySet)
        (acc1 ++ acc2).view.filterKeys(keep.contains).toMap
      }
    ).mapValues(_.head._1)

  def algorithm3(rddFromFile: RDD[(Int, Int)]) = rddFromFile.aggregateByKey(Set.empty[Int])(
    (acc, element) => if (acc.contains(element)) acc - element else acc + element,
    (acc1, acc2) => acc1.diff(acc2) ++ acc2.diff(acc1)
  ).mapValues(_.head)

  def saveWithOverwrite[T](rdd: RDD[(T, T)], path: String)(implicit sc: SparkContext): Unit = {
    val outputPath = new Path(path)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)

    if (hdfs.exists(outputPath)) hdfs.delete(outputPath, true)

    rdd
      .map(x => s"${x._1}\t${x._2}")
      .saveAsTextFile(path)
  }

  def rddFromFile(implicit spark: SparkSession) = spark.sparkContext.textFile("src/main/resources/input/*").map {
    case CommaPattern(num1, num2) => Some((num1.toInt, num2.toInt))
    case TabPattern(num1, num2) => Some((num1.toInt, num2.toInt))
    case HeaderTabPattern(_, _) => None
    case HeaderCommaPattern(_, _) => None
  }.collect { case Some(x) => x }

  val HeaderCommaPattern = """(.*),(.*)""".r
  val HeaderTabPattern = """(.*)\t(.*)""".r
  val CommaPattern = """(\d+),(\d+)""".r
  val TabPattern = """(\d+)\t(\d+)""".r

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("OddNumberIdentifier")
      .getOrCreate()

    implicit val sparkContext = spark.sparkContext



    saveWithOverwrite(algorithm1(rddFromFile), "src/main/resources/algorithm1.csv")
    saveWithOverwrite(algorithm2(rddFromFile), "src/main/resources/algorithm2.csv")
    saveWithOverwrite(algorithm3(rddFromFile), "src/main/resources/algorithm3.csv")
  }

}