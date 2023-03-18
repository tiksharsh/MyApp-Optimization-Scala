package optimazationPkg
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

object Session5StreamingWordCountStateless extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Dont need use below line while executing on spark-shell --master local[2]
  val sc = new SparkContext("local[*]", "word-count")

  // Create Spark Streaming Context
  val  ssc = new StreamingContext(sc, Seconds(5))

  // Lines is DStream
  val lines = ssc.socketTextStream("localhost", 9995)

  //Words are transformed dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairs = words.map(x => (x,1))
  val wordCount = pairs.reduceByKey((x,y) => (x+y))
  wordCount.print()
  ssc.start()
  ssc.awaitTermination()

}
