package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object WindowDistinctDemo {

  case class Order(orderId: String, userName: String, categoryId: String, originalPrice: Double,
                   realPay: Double, status: String, createTime: Long, modifyTime: Long,
                   operator: String, date: String) {
    override def toString: String = this.productIterator.mkString(",")
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("window distinct")
      .getOrCreate()

    import spark.implicits._
    val orderDF = spark.sparkContext.textFile("src/main/resources/data/test2_data.txt")
      .map(line => {
        val fields = line.split(",")
        Order(fields(0), fields(1), fields(2), fields(3).toDouble, fields(4).toDouble, fields(5), fields(6).toLong, fields(7).toLong, fields(8), fields(9))
      }).toDF()
    orderDF.show()

//    import spark.implicits._
//    val orderDS: Dataset[Order] = spark.read.textFile("src/main/resources/data/test2_data.txt").as[Order]
//    orderDS.show()

    import org.apache.spark.sql.functions._
    val distinctOrderDF = orderDF
      //像hive的 row_number() over(partition by order_id order by modify_time desc)
      .withColumn("num", row_number().over(Window.partitionBy($"orderId").orderBy($"modifyTime".desc)))
      .where($"num" === 1)
      .drop("$num")
    distinctOrderDF.show()

    spark.stop()
  }

}
