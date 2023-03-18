package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

// Purpose countByWindow - count the number of lines in ths window


object Session8StreamingCountByWindow extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Don't need use below line while executing on spark-shell --master local[2]
  val sc = new SparkContext("local[*]", "word-count")

  // Create Spark Streaming Context
  val  ssc = new StreamingContext(sc, Seconds(2))

  // Lines is DStream
  val lines = ssc.socketTextStream("localhost", 9995)

  ssc.checkpoint(".")

  //Words are transformed dstream
  val words = lines.countByWindow(Seconds(10), Seconds(2))



  words.print()
  ssc.start()
  ssc.awaitTermination()
}
