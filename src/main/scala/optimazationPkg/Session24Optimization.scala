package optimazationPkg

object Session24Optimization {
  def main(args: Array[String]): Unit = {

  }
println("Harshoday")
//  spark2-shell --conf spark.dynamicAllocation.enabled=false --master yarn --num-executors 6 --executor-cores 2 --executor-memory 3G --confspark.ui.port=4063
//
//  val a = Array(("ERROR",0),("WARN",1))
//  val keyMap = a.toMap
//  val bcast = sc.broadcast(keymap)
//  val rdd = sc.textFile("bigLogLatest.txt")
//  val rdd2 = rdd.map(x => (x.split(":")(0), x.split(":")(1)))
//  val rdd3 = rdd2.map(x => (x._1,x._2, bcast.value(x._1)))
//  rdd3.saveAsTextFile("joinresult2")
//
//
//  val rdd3 = sc.parallelize(a)
//  val rdd4 = rdd2.join(rdd3)
//-------------------------------------------

//  val orderDF = spark.read.format("csv").option("inferSchema", true).option("header", true).option("path", "orders.csv").load
//  orderDF.createOrReplaceTempView("orders")
//  spark.sql("select * from orders").show
//  //  spark2-shell --conf spark.dynamicAllocation.enabled=false --master yarn --num-executors 6 --executor-cores 2 --executor-memory 3G --confspark.ui.port=4063
//  scp wordcount.jar bigdatabysumit@gw02.itversity.com:/home/bigdatabysumit

//--------------------Streaming Session 2-----------------------------







}

