package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Session17StreamingSlidingWindowW16 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("My Streaming Session Week 16")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()


  val orderSchema = StructType(List(
    StructField("order_id",IntegerType),
    StructField("order_date",TimestampType),
    StructField("order_customer_id",IntegerType),
    StructField("order_status",StringType),
    StructField("amount",IntegerType)
  ))
  //1. Read from the Stream using Socket Source (its combination of host + port)-----------------------------

  val ordersDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "12345")
    .load()

  ordersDF.printSchema()


  //2. Process----------------------------------

  val valueDF = ordersDF.select(from_json(col("value"),orderSchema).alias("value"))
  valueDF.printSchema()

  val refinedOrderDF = valueDF.select("value.*")
  refinedOrderDF.printSchema()

  val windowAggDF = refinedOrderDF.withWatermark("order_date", delayThreshold = "30 minute")
    .groupBy(window(col("order_date"),"15 minute","5 minute"))
    .agg(functions.sum("amount").alias("totalInvoice"))

  val outputDF = windowAggDF.select("window.start","window.end","totalInvoice")

  //3. Write to sink data----------------------------------

  val wordCountQuery = outputDF.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoint-location2")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  wordCountQuery.awaitTermination()

}
