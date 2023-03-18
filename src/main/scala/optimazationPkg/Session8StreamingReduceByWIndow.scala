package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

// In ReduceByWindow pair RDD is not required

object Session8StreamingReduceByWIndow extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Don't need use below line while executing on spark-shell --master local[2]
  val sc = new SparkContext("local[*]", "word-count")

  // Create Spark Streaming Context
  val  ssc = new StreamingContext(sc, Seconds(2))

  // Lines is DStream
  val lines = ssc.socketTextStream("localhost", 9995)

  ssc.checkpoint(".")
  // Defining Custom method or function
  def summaryFunc(x: String, y:String)= {
    (x.toInt + y.toInt).toString()
  }

  def inverseFunc(x: String, y:String)= {
    (x.toInt - y.toInt).toString()
  }

  //Words are transformed dstream
  val words = lines.reduceByWindow(summaryFunc, inverseFunc, Seconds(10), Seconds(4))



  words.print()
  ssc.start()
  ssc.awaitTermination()
}


