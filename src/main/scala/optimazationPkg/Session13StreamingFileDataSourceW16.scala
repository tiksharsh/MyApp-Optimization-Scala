package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Session13StreamingFileDataSourceW16 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("My Streaming Session Week 16")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()

  //1. Read from the Stream using File Source (basically you will have folder/directory.
  // new files are detected in this directory and processed)

  val ordersDF = spark.readStream
    .format("json")
    .option("path", "/Users/Wolverine/Documents/BigData-Hadoop/Week 16/DataReadFolder")
    .option("maxFilesPerTrigger", 1)
    .load()

  ordersDF.printSchema()

  //2. Process
  //We are breaking this line into array using split and using explode break array into row

  ordersDF.createOrReplaceTempView("orders")
  val completedOrderDF = spark.sql("select * from orders where order_status = 'COMPLETE' ")



  //3. Write to sink data

  val oredersQuery = completedOrderDF.writeStream
    .format("json")
    .outputMode("append")
    .option("path","/Users/Wolverine/Documents/BigData-Hadoop/Week 16/DataReadFolder/OutPut")
    .option("checkpointLocation", "checkpoint-location1")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  oredersQuery.awaitTermination()


}

