package optimazationPkg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MyProjectYouTube extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","mypractice")
//  val ssc = new StreamingContext(sc, Seconds(5))
//  val file = sc.textFile("hdfs://ms.itversity.com:3306/user/itv002203/Project1/part-m-00000")
////  val rdd1 = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 5/practice.txt")
//  file.collect()

  //RDD
  val rddFromFile = sc.textFile("hdfs://ms.itversity.com:3306/user/itv002203/Project1/part-m-00000.txt")
//  val rddWhole = spark.sparkContext.wholeTextFiles("hdfs://nn1home:8020/text01.txt")
rddFromFile.collect()



}
