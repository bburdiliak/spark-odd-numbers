import model.AppConfig.validate
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

object Main {

  private def saveWithOverwrite[T](rdd: RDD[(T, T)], path: String)(implicit sc: SparkContext): Unit = Try {
    val outputPath = new Path(path)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)

    if (hdfs.exists(outputPath)) hdfs.delete(outputPath, true)

    rdd
      .map(x => s"${x._1}\t${x._2}")
      .saveAsTextFile(path)
  }

  def main(args: Array[String]): Unit = {
    validate(args) match {
      case Left(error) => println(s"Error: ${error}")
      case Right(config) => {
        implicit val spark: SparkSession = SparkSession.builder()
          .master("local[1]")
          .appName("OddNumberIdentifier")
          .getOrCreate()

        implicit val sparkContext = spark.sparkContext

        val rddFromFile = TupleFileReader.read(config.inputPath.toString)
        saveWithOverwrite(config.algorithm.run(rddFromFile), config.output.toString)
      }
    }
  }

}