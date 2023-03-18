package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Session7StreamingReduceByKey extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Don't need use below line while executing on spark-shell --master local[2]
  val sc = new SparkContext("local[*]", "word-count")

  // Create Spark Streaming Context
  val  ssc = new StreamingContext(sc, Seconds(2))

  // Lines is DStream
  val lines = ssc.socketTextStream("localhost", 9995)

  ssc.checkpoint(".")
  // Defining Custom method or function


  //Words are transformed dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairs = words.map(x => (x,1))
                   .reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(10), Seconds(4))
                   .filter(x => x._2 > 0)


  pairs.print()
  ssc.start()
  ssc.awaitTermination()
}

