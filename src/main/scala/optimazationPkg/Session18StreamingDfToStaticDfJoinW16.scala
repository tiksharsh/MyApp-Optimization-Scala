package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Session18StreamingDfToStaticDfJoinW16 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Streaming Join Application")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()


  //Schema Creation for data
  val transactionSchema = StructType(List(
    StructField("card_id",LongType),
    StructField("amount",IntegerType),
    StructField("postcode ",IntegerType),
    StructField("pos_id",LongType),
    StructField("transaction_dt",TimestampType)
  ))

  //1. Read from the Stream using Socket Source (its combination of host + port)-----------------------------

  val transactionDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "12345")
    .load()

  //2. Process----------------------------------

  val valueDF = transactionDF.select(from_json(col("value"),transactionSchema).alias("value"))
  val refinedTransactionDF = valueDF.select("value.*")

  //3. Load Static Dataframe----------------------------------

  val memberDF = spark.readStream
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","/Users/Wolverine/Documents/BigData-Hadoop/Week 16/DataSets/member_details")
    .load()

  //4. Join Condition ----------------------------------

  val joinedExpr = refinedTransactionDF.col("card_id") === memberDF.col("card_id")
  val joinType = "inner"
  val enrichedDF = refinedTransactionDF.join(memberDF,joinedExpr, joinType)
    .drop(memberDF.col("card_id"))

  //5. Write to sink data----------------------------------

  val transactionQuery = enrichedDF.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "checkpoint-location3")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  transactionQuery.awaitTermination()

}
