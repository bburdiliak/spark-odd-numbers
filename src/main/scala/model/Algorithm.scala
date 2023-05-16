package model
import org.apache.spark.rdd.RDD


sealed trait Algorithm {
  def run(rddFromFile: RDD[(Int, Int)]): RDD[(Int, Int)]
}

object Algorithm {
  case object Xor extends Algorithm {
    override def run(rddFromFile: RDD[(Int, Int)]) =
      rddFromFile.reduceByKey(_ ^ _) // XOR bitwise cancels out the same numbers
  }
  case object AggregateWithMap extends Algorithm {
    override def run(rddFromFile: RDD[(Int, Int)]) =
      rddFromFile.aggregateByKey(Map.empty[Int, Boolean])(
        (acc, element) => if (acc.contains(element)) acc - element else acc + (element -> true),
        (acc1, acc2) => {
          val keep = acc1.keySet.diff(acc2.keySet) ++ acc2.keySet.diff(acc1.keySet)
          (acc1 ++ acc2).view.filterKeys(keep.contains).toMap
        }
      ).mapValues(_.head._1)
  }
  case object GroupAndFilter extends Algorithm {
    override def run(rddFromFile: RDD[(Int, Int)]) =
      rddFromFile.
        groupByKey().mapValues(numbers =>
          numbers.groupBy(identity)
            .filter(kv => kv._2.size % 2 == 1)
            .keys.head
        )
  }
  def apply(algorithm: String): Option[Algorithm] = algorithm match {
    case "1" => Some(Xor)
    case "2" => Some(AggregateWithMap)
    case "3" => Some(GroupAndFilter)
    case _ => None
  }

}
