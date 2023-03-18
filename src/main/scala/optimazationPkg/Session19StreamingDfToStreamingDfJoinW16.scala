package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Session19StreamingDfToStreamingDfJoinW16 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Streaming Join Application")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()


  //Schema Creation for data
  val impressionSchema = StructType(List(
    StructField("ImpressionID",StringType),
    StructField("ImpressionTime",TimestampType),
    StructField("CampaignName ",StringType)
  ))

  val clicksSchema = StructType(List(
    StructField("ClickID",StringType),
    StructField("ClickTime",TimestampType)
  ))

  //1. Read from the Stream using Socket Source (its combination of host + port)-----------------------------

  val impressionDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "12345")
    .load()

  val clicksDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "12346")
    .load()

  //2. Process----------------------------------
  //Structure the data based on the schema defined - ImprssionDF
  val valueDF = impressionDF.select(from_json(col("value"),impressionSchema).alias("value"))
  val refinedImpressionDF = valueDF.select("value.*").withWatermark("ImpressionTime", delayThreshold = "30 minute")


  //Structure the data based on the schema defined - ClicksDF
  val valueDF1 = clicksDF.select(from_json(col("value"),clicksSchema).alias("value"))
  val refinedClickDF = valueDF1.select("value.*").withWatermark("ClickTime", delayThreshold = "30 minute")


  //3. Join Condition ----------------------------------

  val joinedExpr = refinedImpressionDF.col("ImpressionID") === refinedClickDF.col("ClickID")
  val joinType = "inner"
  val joinedDF = refinedImpressionDF.join(refinedClickDF,joinedExpr, joinType)
    .drop(refinedClickDF.col("ClickID"))

  //4. Write to sink data----------------------------------

  val campaignQuery = joinedDF.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoint-location4")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  campaignQuery.awaitTermination()

}
