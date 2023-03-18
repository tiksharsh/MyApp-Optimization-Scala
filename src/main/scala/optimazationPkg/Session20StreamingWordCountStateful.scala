package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Session20StreamingWordCountStateful extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Dont need use below line while executing on spark-shell --master local[2]
  val sc = new SparkContext("local[*]", "word-count")

  // Create Spark Streaming Context
  val  ssc = new StreamingContext(sc, Seconds(5))

  // Lines is DStream
  val lines = ssc.socketTextStream("localhost", 9995)

  ssc.checkpoint(".")
  // Defining Custom method or function
  def updateFunction(newValues: Seq[Int], previousState: Option[Int]): Option[Int] = {
    val newCount = previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  //Words are transformed dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairs = words.map(x => (x,1))
  val wordCount = pairs.updateStateByKey(updateFunction)
  wordCount.print()
  ssc.start()
  ssc.awaitTermination()
}
