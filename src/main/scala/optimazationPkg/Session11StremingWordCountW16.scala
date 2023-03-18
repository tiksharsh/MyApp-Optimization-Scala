package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Session11StremingWordCountW16 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("My Streaming Session Week 16")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  //1. Read from the Stream using Socket Source (its combination of host + port)

  val linesDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "12345")
    .load()

  linesDF.printSchema()

  //2. Process
  //We are breaking this line into array using split and using explode break array into row

  val wordDF = linesDF.selectExpr("explode(split(value,' '))as word")
  val countDF = wordDF.groupBy("word").count() // word is column name


  //3. Write to sink data

  val wordCountQuery = countDF.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoint-location1")
    .start()

  wordCountQuery.awaitTermination()


}
