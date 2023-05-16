package model

import java.nio.file.{Files, Path, Paths}
import scala.util.Try

case class AppConfig(inputPath: Path, output: Path, algorithm: Algorithm)

object AppConfig {

  def validate(args: Array[String]): Either[String, AppConfig] = {
    if (args.length != 3)
      Left("Usage: Main <input path> <output path> <algorithm>\n" +
        "where <algorithm> is one of: 1, 2, 3\n" +
        "\n" +
        s"${args.size} argument${if (args.size == 1) "" else "s"} given: ${args.mkString(", ")}"
      )
    else {

      for {
        inputPath <- Try(Paths.get(args(0))).fold(e => Left(s"Invalid input path - ${e.getMessage}"), Right(_))
        inputDirectory <- Either.cond(Files.isDirectory(inputPath), inputPath, "Input path is not a directory")

        outputPath <- Try(Paths.get(args(1))).fold(e => Left(s"Invalid output path - ${e.getMessage}"), Right(_))
        outputDirectory <- Option(outputPath.getParent).toRight("Invalid output path")
        _ = if (!outputDirectory.toFile.exists()) Files.createDirectories(outputDirectory)
        algorithm <- Algorithm(args(2)).toRight("Invalid algorithm")
      } yield AppConfig(inputDirectory, outputPath, algorithm)
    }
  }

}